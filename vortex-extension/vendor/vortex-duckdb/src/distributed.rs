// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Portable state for Vane's explicit distributed table-scan contract.
//!
//! This module deliberately serializes only owned data. Runtime readers and
//! filesystems are reconstructed from an explicit assigned file set when the
//! real worker execution context initializes the scan.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use prost::Message;
use vortex::dtype::DType;
use vortex::dtype::proto::dtype as pb_dtype;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::expr::Expression;
use vortex::expr::proto::ExprSerializeProtoExt;
use vortex::proto::expr as pb_expr;
use vortex::scan::DataSource;

use crate::SESSION;
use crate::convert::PushedAggregate;
use crate::multi_file::BoundFile;
use crate::multi_file::build_bound_file_scan;
use crate::projection::extract_schema_from_dtype;
use crate::table_function::ColumnAggregate;
use crate::table_function::TableFunctionBind;
use crate::table_function::TableFunctionGlobal;

const PORTABLE_BIND_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Message)]
struct ProjectionProto {
    #[prost(uint64, tag = "1")]
    field_index: u64,
    #[prost(bytes = "vec", tag = "2")]
    expression: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AggregateProto {
    #[prost(uint64, tag = "1")]
    projection_id: u64,
    #[prost(uint32, tag = "2")]
    kind: u32,
}

#[derive(Clone, PartialEq, Message)]
struct FileProto {
    #[prost(string, tag = "1")]
    source_url: String,
    #[prost(string, tag = "2")]
    path: String,
    #[prost(uint64, optional, tag = "3")]
    size: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct PortableBindProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    dtype: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    projections: Vec<ProjectionProto>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    filters: Vec<Vec<u8>>,
    #[prost(message, repeated, tag = "5")]
    aggregates: Vec<AggregateProto>,
    #[prost(bool, tag = "6")]
    has_non_optional_filter: bool,
    #[prost(message, repeated, tag = "7")]
    files: Vec<FileProto>,
}

pub struct PortableDistributedBind {
    pub encoded: Vec<u8>,
    pub files: Vec<BoundFile>,
    pub aggregate_scan: bool,
}

pub struct DistributedRuntimeGlobal {
    pub bind_data: TableFunctionBind,
    pub global_data: TableFunctionGlobal,
}

fn encode_expression(expression: &Expression) -> VortexResult<Vec<u8>> {
    Ok(expression.serialize_proto()?.encode_to_vec())
}

fn decode_expression(bytes: &[u8]) -> VortexResult<Expression> {
    let proto = pb_expr::Expr::decode(bytes)
        .map_err(|error| vortex_err!("Invalid distributed Vortex expression: {error}"))?;
    Expression::from_proto(&proto, &SESSION)
}

fn encode_aggregate(aggregate: &ColumnAggregate) -> AggregateProto {
    match aggregate {
        ColumnAggregate::Real {
            projection_id,
            aggregate,
        } => AggregateProto {
            projection_id: *projection_id,
            kind: match aggregate {
                PushedAggregate::Min => 1,
                PushedAggregate::Max => 2,
                PushedAggregate::Sum => 3,
                PushedAggregate::Mean => 4,
                PushedAggregate::First => 5,
                PushedAggregate::Count => 6,
            },
        },
        ColumnAggregate::CountStar => AggregateProto {
            projection_id: 0,
            kind: 7,
        },
    }
}

fn decode_aggregate(aggregate: AggregateProto) -> VortexResult<ColumnAggregate> {
    let pushed = match aggregate.kind {
        1 => PushedAggregate::Min,
        2 => PushedAggregate::Max,
        3 => PushedAggregate::Sum,
        4 => PushedAggregate::Mean,
        5 => PushedAggregate::First,
        6 => PushedAggregate::Count,
        7 => {
            if aggregate.projection_id != 0 {
                vortex_bail!(
                    "Distributed Vortex count-star aggregate has an invalid projection id"
                );
            }
            return Ok(ColumnAggregate::CountStar);
        }
        kind => vortex_bail!("Unknown distributed Vortex aggregate kind: {kind}"),
    };
    Ok(ColumnAggregate::Real {
        projection_id: aggregate.projection_id,
        aggregate: pushed,
    })
}

fn decode_proto(bytes: &[u8]) -> VortexResult<PortableBindProto> {
    let proto = PortableBindProto::decode(bytes)
        .map_err(|error| vortex_err!("Invalid distributed Vortex bind data: {error}"))?;
    if proto.version != PORTABLE_BIND_VERSION {
        vortex_bail!(
            "Unsupported distributed Vortex bind version: {}",
            proto.version
        );
    }
    Ok(proto)
}

pub fn serialize_bind(bind_data: &TableFunctionBind) -> VortexResult<PortableDistributedBind> {
    let dtype = pb_dtype::DType::try_from(bind_data.data_source.dtype())?.encode_to_vec();
    let projections = bind_data
        .column_fields
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| {
            field.projection_expr.as_ref().map(|expression| {
                Ok(ProjectionProto {
                    field_index: u64::try_from(field_index)?,
                    expression: encode_expression(expression)?,
                })
            })
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let filters = bind_data
        .filter_exprs
        .iter()
        .map(encode_expression)
        .collect::<VortexResult<Vec<_>>>()?;
    let files = bind_data.files.clone();
    let proto = PortableBindProto {
        version: PORTABLE_BIND_VERSION,
        dtype,
        projections,
        filters,
        aggregates: bind_data.aggregates.iter().map(encode_aggregate).collect(),
        has_non_optional_filter: bind_data
            .has_non_optional_filter
            .load(std::sync::atomic::Ordering::Relaxed),
        files: files
            .iter()
            .map(|file| FileProto {
                source_url: file.source_url.clone(),
                path: file.path.clone(),
                size: file.size,
            })
            .collect(),
    };
    Ok(PortableDistributedBind {
        encoded: proto.encode_to_vec(),
        files,
        aggregate_scan: !bind_data.aggregates.is_empty(),
    })
}

pub fn deserialize_runtime_bind(
    bytes: &[u8],
    assigned_file_indexes: &[u64],
) -> VortexResult<TableFunctionBind> {
    let proto = decode_proto(bytes)?;
    let dtype_proto = pb_dtype::DType::decode(proto.dtype.as_slice())
        .map_err(|error| vortex_err!("Invalid distributed Vortex dtype: {error}"))?;
    let dtype = DType::from_proto(&dtype_proto, &SESSION)?;
    let mut column_fields = extract_schema_from_dtype(&dtype)?;

    let mut projected_fields = HashSet::new();
    for projection in proto.projections {
        let field_index = usize::try_from(projection.field_index)?;
        let field = column_fields.get_mut(field_index).ok_or_else(|| {
            vortex_err!("Distributed Vortex projection references unknown field {field_index}")
        })?;
        if !projected_fields.insert(field_index) {
            vortex_bail!(
                "Distributed Vortex bind contains duplicate projection field {field_index}"
            );
        }
        field.projection_expr = Some(decode_expression(&projection.expression)?);
    }

    let filter_exprs = proto
        .filters
        .iter()
        .map(|filter| decode_expression(filter))
        .collect::<VortexResult<Vec<_>>>()?;
    let aggregates = proto
        .aggregates
        .into_iter()
        .map(decode_aggregate)
        .collect::<VortexResult<Vec<_>>>()?;
    for aggregate in &aggregates {
        if let ColumnAggregate::Real { projection_id, .. } = aggregate
            && usize::try_from(*projection_id)? >= column_fields.len()
        {
            vortex_bail!(
                "Distributed Vortex aggregate references unknown field {}",
                projection_id
            );
        }
    }

    let files = proto
        .files
        .into_iter()
        .map(|file| {
            if file.source_url.is_empty() || file.path.is_empty() {
                vortex_bail!("Distributed Vortex bind contains an empty file identity");
            }
            Ok(BoundFile {
                source_url: file.source_url,
                path: file.path,
                size: file.size,
            })
        })
        .collect::<VortexResult<Vec<_>>>()?;

    let mut seen = HashSet::new();
    let mut selected_files = Vec::with_capacity(assigned_file_indexes.len());
    let mut file_indexes = Vec::with_capacity(assigned_file_indexes.len());
    for &file_index in assigned_file_indexes {
        let index = usize::try_from(file_index)?;
        if !seen.insert(index) {
            vortex_bail!("Duplicate distributed Vortex file index: {index}");
        }
        selected_files.push(
            files
                .get(index)
                .ok_or_else(|| vortex_err!("Unknown distributed Vortex file index: {index}"))?
                .clone(),
        );
        file_indexes.push(index);
    }

    let data_source = build_bound_file_scan(&selected_files, Some(dtype.clone()))?;
    if data_source.dtype() != &dtype {
        vortex_bail!(
            "Distributed Vortex file schema differs from the coordinator bind: expected {}, got {}",
            dtype,
            data_source.dtype()
        );
    }
    Ok(TableFunctionBind {
        data_source: Arc::new(data_source),
        files: selected_files,
        file_indexes,
        filter_exprs,
        column_fields,
        has_non_optional_filter: AtomicBool::new(proto.has_non_optional_filter),
        aggregates,
    })
}
