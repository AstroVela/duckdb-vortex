// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::LazyLock;

use async_trait::async_trait;
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::registry::ObjectStoreRegistry;
use url::Url;
use vortex::cloud::Registry;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::file::VortexFile;
use vortex::file::VortexOpenOptions;
use vortex::file::multi::open_cached;
use vortex::file::multi::parse_uri_or_path;
use vortex::io::compat::Compat;
use vortex::io::filesystem::FileListing;
use vortex::io::filesystem::FileSystemRef;
use vortex::io::object_store::ObjectStoreFileSystem;
use vortex::io::runtime::BlockingRuntime;
use vortex::layout::LayoutReaderRef;
use vortex::layout::scan::multi::LayoutReaderFactory;
use vortex::layout::scan::multi::MultiLayoutDataSource;

use crate::RUNTIME;
use crate::SESSION;
use crate::duckdb::BindInputRef;
use crate::duckdb::ExtractedValue;

/// Process-wide registry, so repeated scans against the same bucket share one client.
static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// One exact file selected by the coordinator bind. `source_url` identifies
/// the filesystem mount while `path` is the literal path inside that mount.
/// Keeping both avoids reconstructing ambiguous URLs for stores such as hf://.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundFile {
    pub source_url: String,
    pub path: String,
    pub size: Option<u64>,
}

pub struct BoundMultiFileScan {
    pub data_source: MultiLayoutDataSource,
    pub files: Vec<BoundFile>,
}

fn resolve_filesystem(glob_url: &Url) -> VortexResult<(FileSystemRef, String)> {
    // Compat makes us use tokio which is very bad for local reads on
    // high-core machines because reads go into blocking pool
    if glob_url.scheme() == "file" {
        return Ok((
            Arc::new(ObjectStoreFileSystem::local(RUNTIME.handle())),
            glob_url.path().trim_start_matches('/').to_string(),
        ));
    }

    // The full URL goes through the shared registry, which reports the glob as a path *within*
    // the store it returns. For most schemes the store is mounted at the URL authority, so the
    // path is the whole URL path — but not for all of them: an `hf://` store is rooted at a
    // repository and revision, which occupy path segments. Only the registry knows how deep the
    // store is mounted, so globbing anything other than the path it reports would address the
    // wrong keys. Going through the registry also means DuckDB resolves the same set of schemes
    // as the Python and Java bindings, including the OpenDAL-backed ones when the `opendal`
    // feature is on. The registry caches one client per store prefix, so repeated scans against
    // the same bucket or repository share a client even though the filesystem wrapper is rebuilt.
    let (object_store, path) = REGISTRY.resolve(glob_url)?;

    Ok((
        Arc::new(ObjectStoreFileSystem::new(
            Arc::new(Compat::new(object_store)),
            RUNTIME.handle(),
        )),
        // Match MultiFileDataSource::with_glob: ObjectStoreFileSystem paths
        // are relative to the store root, including for absolute file:// URLs.
        path.to_string().trim_start_matches('/').to_string(),
    ))
}

async fn verify_file(file: &BoundFile, fs: &FileSystemRef) -> VortexResult<FileListing> {
    let listing = fs
        .head(&file.path)
        .await?
        .ok_or_else(|| vortex_err!("Bound Vortex file no longer exists: {}", file.path))?;
    if let Some(expected_size) = file.size
        && listing.size != Some(expected_size)
    {
        vortex_bail!(
            "Bound Vortex file size changed for {}: expected {}, got {:?}",
            file.path,
            expected_size,
            listing.size
        );
    }
    Ok(listing)
}

async fn open_bound_file(file: &BoundFile) -> VortexResult<VortexFile> {
    let source_url = Url::parse(&file.source_url).map_err(|error| {
        vortex_err!(
            "Invalid bound Vortex source URL '{}': {error}",
            file.source_url
        )
    })?;
    let (fs, _) = resolve_filesystem(&source_url)?;
    let listing = verify_file(file, &fs).await?;
    let source = fs.open_read(&listing.path).await?;
    open_cached(
        &SESSION,
        source,
        &listing.path,
        listing.size,
        &|options: VortexOpenOptions| options,
    )
    .await
}

struct BoundFileReaderFactory {
    file: BoundFile,
}

#[async_trait]
impl LayoutReaderFactory for BoundFileReaderFactory {
    async fn open(&self) -> VortexResult<Option<LayoutReaderRef>> {
        Ok(Some(open_bound_file(&self.file).await?.layout_reader()?))
    }
}

/// Build a reader over an already selected file set. No glob is evaluated
/// here, so an empty assignment stays empty and a worker cannot discover
/// files that were not part of the coordinator bind.
pub fn build_bound_file_scan(
    files: &[BoundFile],
    empty_dtype: Option<DType>,
) -> VortexResult<MultiLayoutDataSource> {
    if files.is_empty() {
        let dtype = empty_dtype.ok_or_else(|| vortex_err!("No files matched the Vortex scan"))?;
        return Ok(MultiLayoutDataSource::new_deferred(
            dtype,
            Vec::new(),
            Vec::new(),
            &SESSION,
        ));
    }

    RUNTIME.block_on(async {
        let first = open_bound_file(&files[0]).await?.layout_reader()?;
        let remaining = files[1..]
            .iter()
            .cloned()
            .map(|file| Arc::new(BoundFileReaderFactory { file }) as Arc<dyn LayoutReaderFactory>)
            .collect();
        let byte_sizes = files.iter().map(|file| file.size).collect();
        Ok(MultiLayoutDataSource::new_with_first(
            first, remaining, byte_sizes, &SESSION,
        ))
    })
}

/// Shared bind logic for both single-glob and multi-glob variants.
pub fn bind_multi_file_scan(input: &BindInputRef) -> VortexResult<BoundMultiFileScan> {
    let glob_url_parameter = input
        .get_parameter(0)
        .ok_or_else(|| vortex_err!("Missing file glob parameter"))?;

    // The input to the table function can either be a single glob, or a List of glob patterns.
    let glob_strings: Vec<String> = match glob_url_parameter.extract() {
        ExtractedValue::Varchar(glob) => {
            vec![glob.to_string()]
        }
        ExtractedValue::List(globs) => globs
            .into_iter()
            .map(|glob| {
                let ExtractedValue::Varchar(string) = glob.extract() else {
                    vortex_bail!("list element must be Varchar type")
                };

                Ok(string.to_string())
            })
            .try_collect()?,
        _ => vortex_bail!("Invalid argument to read_vortex table function"),
    };

    // Parse each glob URL and resolve its filesystem.
    let mut glob_urls: Vec<Url> = Vec::with_capacity(glob_strings.len());
    for glob_str in &glob_strings {
        glob_urls.push(parse_uri_or_path(glob_str)?);
    }

    let files = RUNTIME.block_on(async {
        let mut files = Vec::new();
        for glob_url in &glob_urls {
            let (fs, glob) = resolve_filesystem(glob_url)?;
            let mut listings = fs.glob(&glob)?.try_collect::<Vec<_>>().await?;
            // FileSystem::list does not promise an order. Freeze a canonical
            // order per user-supplied glob so file_index and task_id remain
            // stable across independent binds and retries.
            listings.sort();
            files.extend(listings.into_iter().map(|listing| BoundFile {
                source_url: glob_url.to_string(),
                path: listing.path,
                size: listing.size,
            }));
        }
        Ok::<_, vortex::error::VortexError>(files)
    })?;
    if files.is_empty() {
        vortex_bail!("No files matched the glob pattern(s): {:?}", glob_strings);
    }
    let data_source = build_bound_file_scan(&files, None)?;
    Ok(BoundMultiFileScan { data_source, files })
}
