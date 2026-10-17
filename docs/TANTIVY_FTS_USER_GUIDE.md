# DuckDB Tantivy 全文检索用户文档

## 构建

使用该分支固定的 Vortex commit，在已有扩展构建命令中加入 `-DVORTEX_ENABLE_INDEX_TANTIVY=ON`。原生 Rust feature 为 `index-tantivy`，普通与 Vane 扩展均注册 `tantivy.static` provider。默认关闭，不要求 SPFresh/hnswlib 的 C++ 依赖。必须使用与扩展版本和平台一致的 DuckDB；本地未签名扩展使用 `duckdb -unsigned`。

```sql
LOAD '/build/vortex.duckdb_extension';
COPY (SELECT 1::BIGINT id, 'Rust search' title, 'full text engine' body)
TO '/data/a.vortex' (FORMAT vortex);
COPY (SELECT 2::BIGINT id, 'Vortex engine' title, 'search in Rust' body)
TO '/data/b.vortex' (FORMAT vortex);
SELECT * FROM vortex_fts_build(['/data/a.vortex','/data/b.vortex'],
                              '/data/articles.json', ['title','body']);
SELECT rank, score, "row".id, "row".title
FROM vortex_fts_search('/data/articles.json', 'rust AND search', 10,
  query_options := '{"field_boosts":{"title":2.0}}') ORDER BY rank;
```

源文件和 reference 使用无 symlink 的绝对本地路径。reference 的父目录须存在，reference 必须不存在。输入 1..4096 个唯一文件，schema 一致；1..16 个 UTF-8 字段，ASCII 列名；保留 `_vortex_row_id` 和 `_vortex_file_id`。

build 返回 reference、generation、rows。search 返回 rank、file_id、row_offset、score、row STRUCT。BM25 越大越相关，平分按行地址升序；所有文件共享一个 BM25 语料。rank 从 1 起，file_id 按文件列表从 1 起，row_offset 从 0 起。NULL 文本无词项，但 `*` 查询保留该文档。

支持短语、布尔、字段查询和正有限 field_boosts。query/query_options 各 ≤64 KiB，k 为 1..10000。总源 ≤512 MiB，索引 ≤1 GiB，文档 ≤100 万。外层 WHERE 是 top-k 之后的过滤，不会改变索引候选集合。

## 重用与失效

默认 strict 每次校验 reference、源文件和索引 artifact；prepared statement 可以重用后端。可在有存活 prepared owner 的查询显式选择 `validation_mode := 'snapshot'`，保留首次验证的源快照。索引源不可变，新数据需新 reference 重建；strict 模式检测到源被替换即拒绝。EXPLAIN/build bind 不会写索引。

不要混用 `vortex_fts_*` 与 vector `vortex_index_*`，前者输出 score，后者输出 distance。它们共享生命周期和访问策略，但验证不同 capability。错误查询、未知字段/options、索引损坏和禁止外部文件访问均会报错。

## Iceberg

使用配套 `duckdb-iceberg/tools/iceberg_fts.py` build/search。工具绑定 table UUID + snapshot + schema/field ID，通过正常 Iceberg scan 处理删除，再物化 Vortex 并调用以上 SQL。结果属于指定快照，行地址属于物化文件。新快照需要新目录重建；无自动 latest 跟踪或优化器重写。见配套仓库 `docs/TANTIVY_FTS_USER_GUIDE.md`。
