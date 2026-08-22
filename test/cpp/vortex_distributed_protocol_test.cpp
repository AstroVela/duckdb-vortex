// SPDX-FileCopyrightText: 2026 Vortex contributors
// SPDX-License-Identifier: Apache-2.0

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/distributed/pipeline_node/pipeline_node.hpp"
#include "duckdb/execution/distributed/pipeline_node/copy_finish.hpp"
#include "duckdb/execution/distributed/pipeline_node/translator.hpp"
#include "duckdb/execution/distributed/pipeline_node/translator_scan.hpp"
#include "duckdb/execution/distributed/plan/scan_split.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace duckdb;

namespace {

using ResultRow = std::pair<int64_t, uint64_t>;
using AggregateRow = std::tuple<int64_t, int64_t, int64_t>;

void Check(bool condition, const string &message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

string SQLString(const std::filesystem::path &path) {
	auto text = path.string();
	return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

struct TempDirectory {
	explicit TempDirectory(std::filesystem::path path_p) : path(std::move(path_p)) {
		std::filesystem::create_directories(path);
	}

	~TempDirectory() {
		std::error_code error;
		std::filesystem::remove_all(path, error);
	}

	std::filesystem::path path;
};

TempDirectory MakeTempDirectory() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	auto path = std::filesystem::temp_directory_path() /
	            ("vortex-distributed-protocol-" + std::to_string(static_cast<unsigned long long>(nonce)));
	return TempDirectory(std::move(path));
}

void RequireQuerySuccess(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (!result || result->HasError()) {
		throw std::runtime_error("query failed: " + sql + ": " + (result ? result->GetError() : "null result"));
	}
}

vector<ResultRow> ReadRows(QueryResult &result) {
	if (result.HasError()) {
		throw std::runtime_error("query execution failed: " + result.GetError());
	}
	auto *materialized = dynamic_cast<MaterializedQueryResult *>(&result);
	Check(materialized != nullptr, "expected a materialized query result");
	Check(materialized->ColumnCount() == 2, "expected two projected Vortex columns");
	idx_t id_column = 0;
	idx_t file_index_column = 1;
	if (materialized->types[0].id() == LogicalTypeId::UBIGINT && materialized->types[1].id() == LogicalTypeId::BIGINT) {
		// A pushed filter on a virtual column can make the standalone physical
		// scan place that filter column before ordinary projections. The full
		// query has an identity/reordering projection above it; this protocol
		// test intentionally executes MakeTableScanPlan's scan-only worker plan.
		id_column = 1;
		file_index_column = 0;
	}
	vector<ResultRow> rows;
	rows.reserve(materialized->RowCount());
	for (idx_t row_index = 0; row_index < materialized->RowCount(); row_index++) {
		rows.emplace_back(materialized->GetValue(id_column, row_index).GetValue<int64_t>(),
		                  materialized->GetValue(file_index_column, row_index).GetValue<uint64_t>());
	}
	std::sort(rows.begin(), rows.end());
	return rows;
}

AggregateRow ReadAggregateRow(QueryResult &result) {
	if (result.HasError()) {
		throw std::runtime_error("aggregate query execution failed: " + result.GetError());
	}
	auto *materialized = dynamic_cast<MaterializedQueryResult *>(&result);
	Check(materialized != nullptr, "expected a materialized aggregate result");
	Check(materialized->ColumnCount() == 3 && materialized->RowCount() == 1, "expected one three-column aggregate row");
	return {materialized->GetValue(0, 0).GetValue<int64_t>(), materialized->GetValue(1, 0).GetValue<int64_t>(),
	        materialized->GetValue(2, 0).GetValue<int64_t>()};
}

void CheckEmptyAggregateRow(QueryResult &result) {
	if (result.HasError()) {
		throw std::runtime_error("empty aggregate query execution failed: " + result.GetError());
	}
	auto *materialized = dynamic_cast<MaterializedQueryResult *>(&result);
	Check(materialized != nullptr, "expected a materialized empty aggregate result");
	Check(materialized->ColumnCount() == 3 && materialized->RowCount() == 1,
	      "expected one three-column empty aggregate row");
	Check(materialized->GetValue(0, 0).IsNull() && materialized->GetValue(1, 0).IsNull() &&
	          materialized->GetValue(2, 0).GetValue<int64_t>() == 0,
	      "unexpected aggregate values for the empty input");
}

struct PlannedScan {
	distributed::DuckPhysicalPlanRef worker_plan;
	vector<distributed::ScanSplit> splits;
	vector<LogicalType> result_types;
	vector<string> result_names;
};

distributed::ScanSplitBatch MakeSplitBatch(vector<distributed::ScanSplit> splits) {
	distributed::ScanSplitBatch batch;
	batch.splits = std::move(splits);
	batch.Validate();
	return batch;
}

distributed::ScanSplitBatch MakeSplitBatch(const distributed::ScanSplit &split) {
	return MakeSplitBatch(vector<distributed::ScanSplit> {split});
}

distributed::ScanSplitBatch MakeEmptySplitBatch(const distributed::ScanSplit &reference) {
	return MakeSplitBatch(
	    distributed::ScanSplit::EmptyExtension(reference.extension_capability, reference.split_codec));
}

void FindTableScans(PhysicalOperator &op, vector<reference<PhysicalTableScan>> &result) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		result.push_back(op.Cast<PhysicalTableScan>());
	}
	for (auto &child : op.children) {
		FindTableScans(child.get(), result);
	}
}

distributed::DuckPhysicalPlanRef ExtractPhysicalPlan(Connection &connection, const string &sql) {
	auto logical_plan = connection.ExtractPlan(sql);
	Check(logical_plan != nullptr, "failed to extract the Vortex logical plan");
	PhysicalPlanGenerator generator(*connection.context);
	auto generated_plan = generator.Plan(std::move(logical_plan));
	Check(generated_plan != nullptr, "failed to generate the Vortex physical plan");
	return distributed::DuckPhysicalPlanRef(generated_plan.release());
}

PlannedScan PlanScan(DuckDB &database, Connection &connection, const string &sql, idx_t worker_slots,
                     const vector<LogicalType> &result_types, const vector<string> &result_names,
                     const string &function_name = "read_vortex") {
	auto coordinator_plan = ExtractPhysicalPlan(connection, sql);
	vector<reference<PhysicalTableScan>> table_scans;
	FindTableScans(coordinator_plan->Root(), table_scans);
	Check(table_scans.size() == 1, "predicate/projection query did not contain exactly one PhysicalTableScan");
	auto &coordinator_scan = table_scans.front().get();
	Check(coordinator_scan.function.name == function_name, "planned the wrong table function");
	Check(coordinator_scan.GetTypes().size() == result_types.size(),
	      "standalone Vortex scan changed the projected column count");
	Check(coordinator_scan.function.HasSerializationCallbacks(), "Vortex bind serde is not registered");
	Check(coordinator_scan.function.HasDistributedScanCallbacks(), "Vortex distributed callbacks are not registered");

	distributed::DuckDBExecutionConfig config;
	config.set_distributed_worker_slots(worker_slots);
	PlannedScan result;
	result.worker_plan = distributed::MakeTableScanPlan(coordinator_scan);
	result.splits = distributed::MakeTableScanSplits(coordinator_scan, config, database.instance);
	// MakeTableScanPlan extracts the scan from any projection wrapper. Use the
	// scan's physical output order when constructing PreparedStatementData.
	result.result_types = result.worker_plan->Root().GetTypes();
	result.result_names = result_names;
	return result;
}

unique_ptr<PhysicalPlan> ClonePlan(const distributed::DuckPhysicalPlanRef &source, Connection &worker,
                                   idx_t scan_node_id) {
	auto plan = make_uniq<PhysicalPlan>(Allocator::DefaultAllocator());
	auto &root = distributed::ClonePhysicalPlanRootIntoPlanOrThrow(source, *plan, "vortex protocol test clone",
	                                                               worker.context.get());
	plan->SetRoot(root);
	auto &scan = plan->Root().Cast<PhysicalTableScan>();
	scan.extra_info.scan_node_id = optional_idx(scan_node_id);
	scan.extra_info.scan_group_id = optional_idx(scan_node_id);
	return plan;
}

unique_ptr<PhysicalPlan> CloneNativePlan(const distributed::DuckPhysicalPlanRef &source, Connection &worker) {
	auto plan = make_uniq<PhysicalPlan>(Allocator::DefaultAllocator());
	auto &root = distributed::ClonePhysicalPlanRootIntoPlanOrThrow(source, *plan, "vortex native round-trip clone",
	                                                               worker.context.get());
	plan->SetRoot(root);
	return plan;
}

unique_ptr<PhysicalPlan> CloneNativePlanWithTransientContext(const distributed::DuckPhysicalPlanRef &source) {
	DuckDB clone_database(nullptr);
	Connection clone_connection(clone_database);
	return CloneNativePlan(source, clone_connection);
}

unique_ptr<PhysicalPlan> ClonePlanWithTransientContext(const distributed::DuckPhysicalPlanRef &source,
                                                       idx_t scan_node_id) {
	DuckDB clone_database(nullptr);
	Connection clone_connection(clone_database);
	return ClonePlan(source, clone_connection, scan_node_id);
}

void ApplySplitBatch(PhysicalPlan &plan, idx_t scan_node_id, const distributed::ScanSplitBatch &batch) {
	unordered_map<idx_t, distributed::ScanSplitBatch> assignments;
	assignments.emplace(scan_node_id, batch);
	string error;
	Check(distributed::ApplyScanSplitBatchesToPlan(plan, assignments, &error),
	      "failed to apply Vortex split batch: " + error);
	Check(distributed::ValidateDistributedScanSplitsApplied(plan, &error),
	      "applied Vortex split batch did not validate: " + error);
}

unique_ptr<QueryResult> ExecutePlan(Connection &worker, unique_ptr<PhysicalPlan> plan, const vector<LogicalType> &types,
                                    const vector<string> &names, const string &label,
                                    StatementType statement_type = StatementType::SELECT_STATEMENT) {
	auto prepared = make_shared_ptr<PreparedStatementData>(statement_type);
	prepared->names = names;
	prepared->types = types;
	prepared->properties.return_type = StatementReturnType::QUERY_RESULT;
	prepared->output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	prepared->memory_type = QueryResultMemoryType::IN_MEMORY;
	prepared->physical_plan = std::move(plan);
	PendingQueryParameters parameters;
	auto pending = worker.context->PendingQueryPreparedStatementNoRebind(label, prepared, parameters);
	Check(pending != nullptr, "worker returned no pending query");
	if (pending->HasError()) {
		throw std::runtime_error("worker rejected physical plan: " + pending->GetError());
	}
	return pending->Execute();
}

void CheckCopyStatistics(QueryResult &result, const std::filesystem::path &path, idx_t expected_rows) {
	if (result.HasError()) {
		throw std::runtime_error("Vortex COPY statistics query failed: " + result.GetError());
	}
	auto *materialized = dynamic_cast<MaterializedQueryResult *>(&result);
	Check(materialized != nullptr, "expected materialized Vortex COPY statistics");
	Check(materialized->ColumnCount() == 6 && materialized->RowCount() == 1,
	      "Vortex COPY did not return one written-file statistics row");
	Check(materialized->GetValue(0, 0).GetValue<string>() == path.string(),
	      "Vortex COPY statistics returned the wrong file path");
	Check(materialized->GetValue(1, 0).GetValue<uint64_t>() == expected_rows,
	      "Vortex COPY statistics returned the wrong row count");
	auto reported_size = materialized->GetValue(2, 0).GetValue<uint64_t>();
	Check(reported_size > 0, "Vortex COPY statistics returned an empty file size");
	Check(reported_size == std::filesystem::file_size(path),
	      "Vortex COPY statistics file size does not match the written artifact");
	Check(materialized->GetValue(3, 0).IsNull(), "Vortex COPY unexpectedly reported a footer size");
	auto column_statistics = materialized->GetValue(4, 0);
	Check(!column_statistics.IsNull() && MapValue::GetChildren(column_statistics).empty(),
	      "Vortex COPY unexpectedly reported column statistics");
	Check(materialized->GetValue(5, 0).IsNull(), "Vortex COPY unexpectedly reported partition keys");
}

void TestWriteProtocol() {
	auto temp = MakeTempDirectory();
	auto output = temp.path / "roundtrip.vortex";

	DuckDB coordinator_database(nullptr);
	Connection coordinator(coordinator_database);
	auto copy_sql = "COPY (SELECT CAST(range AS BIGINT) AS id, 'row-' || CAST(range AS VARCHAR) AS payload "
	                "FROM range(37)) TO " +
	                SQLString(output) + " (FORMAT VORTEX, RETURN_STATS true)";
	auto coordinator_plan = ExtractPhysicalPlan(coordinator, copy_sql);
	Check(coordinator_plan->Root().type == PhysicalOperatorType::COPY_TO_FILE,
	      "Vortex write did not plan a COPY_TO_FILE root");
	auto &copy_root = coordinator_plan->Root().Cast<PhysicalCopyToFile>();
	Check(copy_root.function.name == "vortex", "Vortex write planned the wrong COPY function");
	Check(copy_root.function.serialize && copy_root.function.deserialize, "Vortex COPY bind serde is not registered");
	Check(copy_root.function.copy_to_get_written_statistics,
	      "Vortex COPY written-file statistics callback is not registered");
	Check(copy_root.return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS,
	      "Vortex COPY RETURN_STATS did not reach the physical plan");
	Check(copy_root.bind_data != nullptr, "Vortex COPY physical bind is missing");
	auto copied_bind = copy_root.bind_data->Copy();
	Check(copied_bind != nullptr, "Vortex COPY bind clone returned null");

	distributed::PlanConfig config;
	config.db = coordinator_database.instance;
	config.config =
	    std::make_shared<distributed::DuckDBExecutionConfig>(distributed::DuckDBExecutionConfig::from_env());
	{
		auto translated =
		    distributed::physical_plan_to_pipeline_node(config, coordinator_plan, coordinator.context.get());
		if (translated.is_err()) {
			throw std::runtime_error(std::string("Vortex distributed COPY translation failed: ") +
			                         translated.error().what());
		}
		auto copy_finish = std::dynamic_pointer_cast<distributed::CopyFinishNode>(translated.value()->inner());
		Check(copy_finish != nullptr, "Vortex distributed COPY did not translate to CopyFinish");
		auto translated_spec = copy_finish->copy_sink()->spec().Clone();
		Check(translated_spec.bind_data != nullptr, "Vortex distributed COPY spec bind clone returned null");
	}

	DuckDB worker_database(nullptr);
	Connection worker(worker_database);
	// Clone through a connection that is destroyed before execution. Vortex's
	// deserialized COPY bind must contain only owned schema data, never a
	// ClientContext or connection-scoped runtime reference.
	auto worker_plan = CloneNativePlanWithTransientContext(coordinator_plan);
	coordinator_plan.reset();

	auto result_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto result_names = GetCopyFunctionReturnNames(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto result = ExecutePlan(worker, std::move(worker_plan), result_types, result_names,
	                          "test:vortex-distributed-copy-roundtrip", StatementType::COPY_STATEMENT);
	Check(result != nullptr, "Vortex COPY worker returned no result");
	CheckCopyStatistics(*result, output, 37);

	auto read_result = worker.Query("SELECT count(*), sum(id), min(payload), max(payload) FROM read_vortex(" +
	                                SQLString(output) + ")");
	Check(read_result != nullptr && !read_result->HasError() && read_result->RowCount() == 1,
	      "Vortex COPY worker artifact could not be read");
	Check(read_result->GetValue(0, 0).GetValue<int64_t>() == 37 &&
	          read_result->GetValue(1, 0).GetValue<int64_t>() == 666 &&
	          read_result->GetValue(2, 0).GetValue<string>() == "row-0" &&
	          read_result->GetValue(3, 0).GetValue<string>() == "row-9",
	      "Vortex COPY worker artifact contents differ from the source relation");

	auto empty_output = temp.path / "empty.vortex";
	auto empty_plan =
	    ExtractPhysicalPlan(coordinator, "COPY (SELECT CAST(range AS BIGINT) AS id FROM range(0)) TO " +
	                                         SQLString(empty_output) + " (FORMAT VORTEX, RETURN_STATS true)");
	auto empty_worker_plan = CloneNativePlanWithTransientContext(empty_plan);
	empty_plan.reset();
	auto empty_result = ExecutePlan(worker, std::move(empty_worker_plan), result_types, result_names,
	                                "test:vortex-distributed-copy-empty", StatementType::COPY_STATEMENT);
	Check(empty_result != nullptr, "empty Vortex COPY worker returned no result");
	CheckCopyStatistics(*empty_result, empty_output, 0);
	auto empty_read = worker.Query("SELECT count(*) FROM read_vortex(" + SQLString(empty_output) + ")");
	Check(empty_read != nullptr && !empty_read->HasError() && empty_read->GetValue(0, 0).GetValue<int64_t>() == 0,
	      "empty Vortex COPY artifact did not round-trip as zero rows");
}

vector<ResultRow> ExecuteAssigned(const PlannedScan &planned, Connection &worker,
                                  const distributed::ScanSplitBatch &batch, idx_t scan_node_id) {
	auto plan = ClonePlan(planned.worker_plan, worker, scan_node_id);
	ApplySplitBatch(*plan, scan_node_id, batch);
	auto result = ExecutePlan(worker, std::move(plan), planned.result_types, planned.result_names,
	                          "test:vortex-distributed-assigned");
	Check(result != nullptr, "assigned worker returned no result");
	return ReadRows(*result);
}

AggregateRow ExecuteAggregateAssigned(const PlannedScan &planned, Connection &worker,
                                      const distributed::ScanSplitBatch &batch, idx_t scan_node_id) {
	auto plan = ClonePlan(planned.worker_plan, worker, scan_node_id);
	ApplySplitBatch(*plan, scan_node_id, batch);
	auto result = ExecutePlan(worker, std::move(plan), planned.result_types, planned.result_names,
	                          "test:vortex-distributed-aggregate-assigned");
	Check(result != nullptr, "aggregate worker returned no result");
	return ReadAggregateRow(*result);
}

idx_t ExecuteAssignedRowCount(const PlannedScan &planned, Connection &worker, const distributed::ScanSplitBatch &batch,
                              idx_t scan_node_id) {
	auto plan = ClonePlan(planned.worker_plan, worker, scan_node_id);
	ApplySplitBatch(*plan, scan_node_id, batch);
	auto result = ExecutePlan(worker, std::move(plan), planned.result_types, planned.result_names,
	                          "test:vortex-distributed-row-count");
	Check(result != nullptr && !result->HasError(), "row-count worker execution failed");
	auto *materialized = dynamic_cast<MaterializedQueryResult *>(result.get());
	Check(materialized != nullptr, "expected a materialized row-count result");
	return materialized->RowCount();
}

void ExpectApplyFailure(const PlannedScan &planned, Connection &worker, const distributed::ScanSplitBatch &batch,
                        const string &expected) {
	try {
		auto plan = ClonePlan(planned.worker_plan, worker, 900);
		unordered_map<idx_t, distributed::ScanSplitBatch> assignments;
		assignments.emplace(900, batch);
		string error;
		if (!distributed::ApplyScanSplitBatchesToPlan(*plan, assignments, &error)) {
			Check(expected.empty() || StringUtil::Contains(error, expected),
			      "unexpected apply error: " + error + ", expected: " + expected);
			return;
		}
		throw std::runtime_error("invalid Vortex split was accepted");
	} catch (const std::exception &error) {
		Check(expected.empty() || StringUtil::Contains(error.what(), expected),
		      "unexpected apply exception: " + string(error.what()) + ", expected: " + expected);
	}
}

void ValidateSplitContract(const vector<distributed::ScanSplit> &splits, idx_t expected_split_count,
                           const string &capability_name = "read_vortex",
                           const LogicalType &parameter_type = LogicalType::LIST(LogicalType::VARCHAR)) {
	Check(!splits.empty(), "Vortex planner returned no splits");
	const auto expected_signature =
	    GetDistributedTableFunctionSignature(capability_name, {parameter_type}, LogicalType::INVALID);
	for (const auto &split : splits) {
		Check(split.kind == distributed::ScanSplitKind::EXTENSION, "Vortex emitted a non-extension split");
		Check(split.file.path.empty(), "Vortex extension split unexpectedly contains an engine file");
		Check(split.extension_capability.extension_name == "vortex", "wrong Vortex capability owner");
		Check(split.extension_capability.capability.name == capability_name, "wrong Vortex capability name");
		Check(split.extension_capability.capability.function_signature == expected_signature,
		      "wrong Vortex capability overload signature");
		Check(split.extension_capability.capability.protocol_version == 1, "wrong Vortex protocol version");
		Check(split.split_codec.name == "vane.vortex-file-split", "wrong Vortex split codec");
		Check(split.split_codec.version == 1, "wrong Vortex split codec version");
	}
	if (expected_split_count == 0) {
		Check(splits.size() == 1 && splits[0].empty, "empty Vortex scan did not produce one explicit empty split");
		Check(splits[0].split_id == "empty", "empty Vortex scan produced a non-canonical empty split id");
		Check(splits[0].extension_payload.empty(), "empty Vortex split unexpectedly contains a payload");
		auto roundtrip =
		    distributed::ScanSplitBatch::DeserializeFromBytes(MakeSplitBatch(splits[0]).SerializeToBytes());
		Check(roundtrip.splits.size() == 1 && roundtrip.splits[0].empty,
		      "empty split batch round-trip changed the empty marker");
		return;
	}
	Check(splits.size() == expected_split_count, "Vortex planned an unexpected elementary split count");
	set<string> split_ids;
	for (const auto &split : splits) {
		Check(!split.empty, "non-empty Vortex scan emitted an empty split");
		Check(!split.split_id.empty(), "empty Vortex split id");
		Check(!split.extension_payload.empty(), "empty Vortex split payload");
		Check(split.estimated_cardinality.IsValid(), "missing Vortex split cardinality estimate");
		Check(split.estimated_bytes.IsValid(), "missing Vortex split byte estimate");
		Check(split_ids.insert(split.split_id).second, "duplicate planned Vortex split id");
		auto roundtrip = distributed::ScanSplitBatch::DeserializeFromBytes(MakeSplitBatch(split).SerializeToBytes());
		Check(roundtrip.splits.size() == 1, "split batch round-trip changed the split count");
		Check(roundtrip.splits[0].split_codec == split.split_codec, "split batch round-trip changed the codec");
	}
}

void TestProtocol() {
	auto temp = MakeTempDirectory();
	vector<std::filesystem::path> files;
	for (idx_t file_index = 0; file_index < 3; file_index++) {
		files.push_back(temp.path / ("part-" + std::to_string(file_index) + ".vortex"));
	}

	DuckDB coordinator_database(nullptr);
	Connection coordinator(coordinator_database);
	for (idx_t file_index = 0; file_index < files.size(); file_index++) {
		auto start = file_index * 10;
		auto sql = "COPY (SELECT CAST(" + std::to_string(start) + " + range AS BIGINT) AS id, 'row-' || CAST(" +
		           std::to_string(start) + " + range AS VARCHAR) AS payload FROM range(10)) TO " +
		           SQLString(files[file_index]) + " (FORMAT VORTEX)";
		RequireQuerySuccess(coordinator, sql);
	}

	string file_list = "[";
	for (idx_t file_index = 0; file_index < files.size(); file_index++) {
		if (file_index > 0) {
			file_list += ", ";
		}
		file_list += SQLString(files[file_index]);
	}
	file_list += "]";

	// DuckDB late materialization rewrites one scan into a self semi-join.
	// Vane schedules explicit splits per physical scan, so a distributed-capable
	// Vortex function must retain one scan until grouped multi-scan assignments
	// are part of the protocol.
	const auto limited_sql = "SELECT id, payload FROM read_vortex(" + file_list + ") ORDER BY id DESC LIMIT 3";
	auto limited_physical_plan = ExtractPhysicalPlan(coordinator, limited_sql);
	vector<reference<PhysicalTableScan>> limited_scans;
	FindTableScans(limited_physical_plan->Root(), limited_scans);
	Check(limited_scans.size() == 1, "distributed Vortex scan enabled unsafe late materialization");
	auto limited_result = coordinator.Query(limited_sql);
	Check(limited_result != nullptr && !limited_result->HasError() && limited_result->RowCount() == 3,
	      "single-scan limited Vortex baseline failed");
	auto limited_scan =
	    PlanScan(coordinator_database, coordinator, limited_sql, 8, limited_result->types, limited_result->names);
	ValidateSplitContract(limited_scan.splits, files.size());
	const auto filtered_limited_sql =
	    "SELECT id, payload FROM read_vortex(" + file_list + ") WHERE id < 15 ORDER BY id DESC LIMIT 3";
	auto filtered_limited_physical_plan = ExtractPhysicalPlan(coordinator, filtered_limited_sql);
	auto filtered_limited_result = coordinator.Query(filtered_limited_sql);
	Check(filtered_limited_result != nullptr && !filtered_limited_result->HasError() &&
	          filtered_limited_result->RowCount() == 3,
	      "filtered limited Vortex baseline failed");
	auto filtered_limited_scan = PlanScan(coordinator_database, coordinator, filtered_limited_sql, 8,
	                                      filtered_limited_result->types, filtered_limited_result->names);
	ValidateSplitContract(filtered_limited_scan.splits, files.size());

	const auto scan_sql = "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE id >= 7 AND id < 25";
	auto baseline_result = coordinator.Query(scan_sql);
	Check(baseline_result != nullptr && !baseline_result->HasError(), "local Vortex baseline failed");
	auto baseline_types = baseline_result->types;
	auto baseline_names = baseline_result->names;
	auto baseline_rows = ReadRows(*baseline_result);
	Check(baseline_rows.size() == 18, "unexpected filtered baseline cardinality");
	int64_t baseline_sum = 0;
	uint64_t baseline_file_sum = 0;
	for (const auto &[id, file_index] : baseline_rows) {
		baseline_sum += id;
		baseline_file_sum += file_index;
	}
	Check(baseline_sum == 279 && baseline_file_sum == 20,
	      "unexpected local Vortex aggregate baseline: sum(id)=" + std::to_string(baseline_sum) +
	          ", sum(file_index)=" + std::to_string(baseline_file_sum));

	auto planned = PlanScan(coordinator_database, coordinator, scan_sql, 8, baseline_types, baseline_names);
	ValidateSplitContract(planned.splits, files.size());
	Check(planned.splits.size() == files.size(), "workers > splits changed elementary split count");

	DuckDB worker_database(nullptr);
	Connection worker(worker_database);
	// DuckDB deliberately deserializes DynamicFilter without its
	// connection-scoped filter_data. A complete native TopN plan must treat that
	// optional hint as unavailable, not dereference it or reject every row.
	auto limited_native_plan = CloneNativePlan(limited_physical_plan, worker);
	auto limited_native_result = ExecutePlan(worker, std::move(limited_native_plan), limited_result->types,
	                                         limited_result->names, "test:vortex-native-topn-roundtrip");
	Check(limited_native_result != nullptr && !limited_native_result->HasError(),
	      "native TopN Vortex physical-plan round-trip failed");
	auto *limited_native_materialized = dynamic_cast<MaterializedQueryResult *>(limited_native_result.get());
	Check(limited_native_materialized != nullptr && limited_native_materialized->RowCount() == 3,
	      "native TopN Vortex physical-plan round-trip returned the wrong row count");
	auto filtered_limited_native_plan = CloneNativePlan(filtered_limited_physical_plan, worker);
	auto filtered_limited_native_result =
	    ExecutePlan(worker, std::move(filtered_limited_native_plan), filtered_limited_result->types,
	                filtered_limited_result->names, "test:vortex-native-filtered-topn-roundtrip");
	Check(filtered_limited_native_result != nullptr && !filtered_limited_native_result->HasError(),
	      "native filtered TopN Vortex physical-plan round-trip failed");
	auto *filtered_limited_native_materialized =
	    dynamic_cast<MaterializedQueryResult *>(filtered_limited_native_result.get());
	Check(filtered_limited_native_materialized != nullptr && filtered_limited_native_materialized->RowCount() == 3 &&
	          filtered_limited_native_materialized->GetValue(0, 0).GetValue<int64_t>() == 14 &&
	          filtered_limited_native_materialized->GetValue(0, 1).GetValue<int64_t>() == 13 &&
	          filtered_limited_native_materialized->GetValue(0, 2).GetValue<int64_t>() == 12,
	      "native filtered TopN Vortex physical-plan round-trip dropped its required predicate");
	idx_t limited_scan_rows = 0;
	for (idx_t split_index = 0; split_index < limited_scan.splits.size(); split_index++) {
		limited_scan_rows += ExecuteAssignedRowCount(
		    limited_scan, worker, MakeSplitBatch(limited_scan.splits[split_index]), 25 + split_index);
	}
	Check(limited_scan_rows == files.size() * 10, "detached TopN scan incorrectly applied an optional dynamic filter");
	idx_t filtered_limited_scan_rows = 0;
	for (idx_t split_index = 0; split_index < filtered_limited_scan.splits.size(); split_index++) {
		filtered_limited_scan_rows += ExecuteAssignedRowCount(
		    filtered_limited_scan, worker, MakeSplitBatch(filtered_limited_scan.splits[split_index]), 35 + split_index);
	}
	Check(filtered_limited_scan_rows == 15,
	      "ignoring a detached TopN dynamic filter also dropped its required sibling predicate");

	// The Vortex-owned bind path must retain ordinary absolute-path glob
	// semantics. Canonical sorting also makes the file index and split payload
	// stable even when the underlying object store lists entries out of order.
	const auto glob_scan_sql = "SELECT id, file_index FROM read_vortex(" + SQLString(temp.path / "part-*.vortex") +
	                           ") WHERE id >= 7 AND id < 25";
	auto glob_baseline_result = coordinator.Query(glob_scan_sql);
	Check(glob_baseline_result != nullptr && !glob_baseline_result->HasError(),
	      "absolute-path Vortex glob baseline failed");
	auto glob_baseline_rows = ReadRows(*glob_baseline_result);
	Check(glob_baseline_rows == baseline_rows, "absolute-path Vortex glob changed local file ordering or results");
	auto glob_plan = PlanScan(coordinator_database, coordinator, glob_scan_sql, 8, glob_baseline_result->types,
	                          glob_baseline_result->names);
	ValidateSplitContract(glob_plan.splits, files.size(), "read_vortex", LogicalType::VARCHAR);
	vector<ResultRow> glob_distributed_rows;
	for (idx_t split_index = 0; split_index < glob_plan.splits.size(); split_index++) {
		const auto &split = glob_plan.splits[split_index];
		Check(split.extension_payload.find("part-" + split.split_id + ".vortex") != string::npos,
		      "glob split id does not match its canonically sorted file payload");
		auto rows = ExecuteAssigned(glob_plan, worker, MakeSplitBatch(split), 50 + split_index);
		glob_distributed_rows.insert(glob_distributed_rows.end(), rows.begin(), rows.end());
	}
	std::sort(glob_distributed_rows.begin(), glob_distributed_rows.end());
	Check(glob_distributed_rows == baseline_rows, "distributed absolute-path Vortex glob differs from baseline");

	// Both public scan names are registered as complete overload sets through
	// Vane's ordinary ExtensionLoader path. Each must publish its own capability
	// and remain executable after worker-plan cloning.
	const auto alias_sql = "SELECT id, file_index FROM vortex_scan(" + file_list + ") WHERE id >= 7 AND id < 25";
	auto alias_baseline_result = coordinator.Query(alias_sql);
	Check(alias_baseline_result != nullptr && !alias_baseline_result->HasError(), "vortex_scan alias baseline failed");
	auto alias_plan = PlanScan(coordinator_database, coordinator, alias_sql, 8, alias_baseline_result->types,
	                           alias_baseline_result->names, "vortex_scan");
	ValidateSplitContract(alias_plan.splits, files.size(), "vortex_scan");
	vector<ResultRow> alias_distributed_rows;
	for (idx_t split_index = 0; split_index < alias_plan.splits.size(); split_index++) {
		auto rows =
		    ExecuteAssigned(alias_plan, worker, MakeSplitBatch(alias_plan.splits[split_index]), 75 + split_index);
		alias_distributed_rows.insert(alias_distributed_rows.end(), rows.begin(), rows.end());
	}
	std::sort(alias_distributed_rows.begin(), alias_distributed_rows.end());
	Check(alias_distributed_rows == baseline_rows, "distributed vortex_scan alias differs from baseline");

	// Ordinary DuckDB plan serialization uses the same bind serde without
	// invoking Vane's create_worker_bind callback. Deserialization must remain I/O-free,
	// then reconstruct the complete already-bound file set in the real execution
	// context instead of requiring a distributed split batch.
	auto native_source = ExtractPhysicalPlan(coordinator, scan_sql);
	auto native_types = native_source->Root().GetTypes();
	auto native_hidden_path = temp.path;
	native_hidden_path += ".native-hidden";
	std::filesystem::rename(temp.path, native_hidden_path);
	unique_ptr<PhysicalPlan> native_roundtrip_plan;
	try {
		native_roundtrip_plan = CloneNativePlan(native_source, worker);
		std::filesystem::rename(native_hidden_path, temp.path);
	} catch (...) {
		std::filesystem::rename(native_hidden_path, temp.path);
		throw;
	}
	auto native_roundtrip_result = ExecutePlan(worker, std::move(native_roundtrip_plan), native_types, baseline_names,
	                                           "test:vortex-native-roundtrip");
	Check(native_roundtrip_result != nullptr && ReadRows(*native_roundtrip_result) == baseline_rows,
	      "native Vortex physical-plan round-trip differs from baseline");

	// Clone while the data directory is unavailable. A hidden call to the
	// original bind would attempt to enumerate these paths and fail. The worker
	// deserialize path must only rebuild owned portable metadata.
	auto hidden_path = temp.path;
	hidden_path += ".hidden";
	std::filesystem::rename(temp.path, hidden_path);
	unique_ptr<PhysicalPlan> lifecycle_plan;
	try {
		lifecycle_plan = ClonePlanWithTransientContext(planned.worker_plan, 100);
		std::filesystem::rename(hidden_path, temp.path);
	} catch (...) {
		std::filesystem::rename(hidden_path, temp.path);
		throw;
	}
	auto first_batch = MakeSplitBatch(planned.splits.front());
	ApplySplitBatch(*lifecycle_plan, 100, first_batch);
	auto retry_plan_ref = distributed::DuckPhysicalPlanRef(lifecycle_plan.release());
	auto retry_clone = ClonePlan(retry_plan_ref, worker, 100);
	// Physical-plan cloning intentionally resets Vane's runtime-applied marker.
	// Re-applying the same split batch must be safe; the bind serde still carries
	// the assignment and cannot broaden it while cloning.
	ApplySplitBatch(*retry_clone, 100, first_batch);
	auto retry_result = ExecutePlan(worker, std::move(retry_clone), baseline_types, baseline_names,
	                                "test:vortex-distributed-retry-clone");
	Check(retry_result != nullptr && !retry_result->HasError(), "retry clone execution failed");

	// Re-applying the identical assignment is idempotent, while changing an
	// already-applied bind in place must fail. FTE retries clone the detached
	// plan; they never mutate one execution from file A into file B.
	Check(planned.splits.size() > 1, "expected multiple splits for reassignment test");
	auto idempotent_plan = ClonePlan(planned.worker_plan, worker, 125);
	ApplySplitBatch(*idempotent_plan, 125, MakeSplitBatch(planned.splits[0]));
	ApplySplitBatch(*idempotent_plan, 125, MakeSplitBatch(planned.splits[0]));
	string reassignment_error;
	try {
		ApplySplitBatch(*idempotent_plan, 125, MakeSplitBatch(planned.splits[1]));
	} catch (const std::exception &error) {
		reassignment_error = error.what();
	}
	Check(StringUtil::Contains(reassignment_error, "different explicit split assignment"),
	      "Vortex accepted a different assignment on an already-applied bind: " + reassignment_error);

	// File identity is resolved only from the split and immutable bind. If
	// the bound object disappears before a retry, fail explicitly instead of
	// re-globbing a replacement or widening the scan.
	auto missing_file = files[0];
	missing_file += ".missing";
	std::filesystem::rename(files[0], missing_file);
	string missing_file_error;
	try {
		(void)ExecuteAssigned(planned, worker, first_batch, 150);
	} catch (const std::exception &error) {
		missing_file_error = error.what();
	}
	std::filesystem::rename(missing_file, files[0]);
	Check(StringUtil::Contains(missing_file_error, "no longer exists"),
	      "missing bound Vortex file did not fail with an identity error: " + missing_file_error);

	// The public Vortex listing gives us a stable path and byte length, but no
	// snapshot or object-version token. At minimum, retries must reject a file
	// whose length no longer matches the coordinator's immutable bind.
	auto original_file_size = std::filesystem::file_size(files[0]);
	{
		std::ofstream changed_file(files[0], std::ios::binary | std::ios::app);
		Check(changed_file.good(), "failed to open a bound Vortex file for the size-change test");
		changed_file.put('\0');
		Check(changed_file.good(), "failed to extend a bound Vortex file for the size-change test");
	}
	string changed_file_error;
	try {
		(void)ExecuteAssigned(planned, worker, first_batch, 175);
	} catch (const std::exception &error) {
		changed_file_error = error.what();
	}
	std::filesystem::resize_file(files[0], original_file_size);
	Check(StringUtil::Contains(changed_file_error, "size changed"),
	      "changed bound Vortex file did not fail with an identity error: " + changed_file_error);

	vector<ResultRow> distributed_rows;
	for (idx_t split_index = 0; split_index < planned.splits.size(); split_index++) {
		auto roundtrip = distributed::ScanSplitBatch::DeserializeFromBytes(
		    MakeSplitBatch(planned.splits[split_index]).SerializeToBytes());
		auto rows = ExecuteAssigned(planned, worker, roundtrip, 200 + split_index);
		distributed_rows.insert(distributed_rows.end(), rows.begin(), rows.end());
	}
	std::sort(distributed_rows.begin(), distributed_rows.end());
	Check(distributed_rows == baseline_rows, "per-file worker results do not equal the local Vortex scan");

	// Exercise state that exists only in the Vortex bind: strlen projection
	// pushdown and a LIKE complex filter that DuckDB removes from the upper plan.
	// Both expressions must survive bind serde without a worker rebind.
	const auto expression_sql =
	    "SELECT strlen(payload), file_index FROM read_vortex(" + file_list + ") WHERE payload LIKE '%2%'";
	auto expression_baseline_result = coordinator.Query(expression_sql);
	Check(expression_baseline_result != nullptr && !expression_baseline_result->HasError(),
	      "projection/complex-filter local Vortex baseline failed");
	auto expression_baseline_rows = ReadRows(*expression_baseline_result);
	Check(expression_baseline_rows.size() == 12, "unexpected projection/complex-filter baseline cardinality");
	auto expression_plan = PlanScan(coordinator_database, coordinator, expression_sql, 8,
	                                expression_baseline_result->types, expression_baseline_result->names);
	ValidateSplitContract(expression_plan.splits, files.size());
	vector<ResultRow> expression_rows;
	for (idx_t split_index = 0; split_index < expression_plan.splits.size(); split_index++) {
		auto rows = ExecuteAssigned(expression_plan, worker, MakeSplitBatch(expression_plan.splits[split_index]),
		                            225 + split_index);
		expression_rows.insert(expression_rows.end(), rows.begin(), rows.end());
	}
	std::sort(expression_rows.begin(), expression_rows.end());
	Check(expression_rows == expression_baseline_rows,
	      "distributed projection/complex-filter result differs from baseline");

	// A file_index predicate can be evaluated without opening readers. The
	// coordinator must omit files that cannot match while preserving the stable
	// original file index in the split id and worker output.
	const auto pruned_sql =
	    "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE file_index = 1 AND id >= 12 AND id < 18";
	auto pruned_baseline_result = coordinator.Query(pruned_sql);
	Check(pruned_baseline_result != nullptr && !pruned_baseline_result->HasError(),
	      "file_index-pruned local Vortex baseline failed");
	auto pruned_baseline_rows = ReadRows(*pruned_baseline_result);
	Check(pruned_baseline_rows.size() == 6, "unexpected file_index-pruned baseline cardinality");
	auto pruned = PlanScan(coordinator_database, coordinator, pruned_sql, 8, pruned_baseline_result->types,
	                       pruned_baseline_result->names);
	ValidateSplitContract(pruned.splits, 1);
	Check(pruned.splits.size() == 1, "file_index predicate did not prune the distributed split set");
	Check(pruned.splits[0].split_id == "1", "file_index pruning changed the stable coordinator split id");
	Check(ExecuteAssigned(pruned, worker, MakeSplitBatch(pruned.splits[0]), 250) == pruned_baseline_rows,
	      "file_index-pruned worker result differs from baseline");
	// A split can be structurally valid and reference a file from the same
	// immutable bind while still being outside this scan's coordinator-pruned
	// split set. Applying such a split must fail rather than broaden the
	// worker scan.
	ExpectApplyFailure(pruned, worker, first_batch, "outside the planned file set");

	// Non-range virtual filters must be evaluated rather than silently treated
	// as Selection::All. Both split planning and runtime reader construction use
	// the same exact file-index predicate evaluation.
	const auto not_equal_sql = "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE file_index != 1";
	auto not_equal_baseline_result = coordinator.Query(not_equal_sql);
	Check(not_equal_baseline_result != nullptr && !not_equal_baseline_result->HasError(),
	      "file_index-not-equal local Vortex baseline failed");
	auto not_equal_baseline_rows = ReadRows(*not_equal_baseline_result);
	Check(not_equal_baseline_rows.size() == 20, "file_index-not-equal local filter returned the wrong row count");
	auto not_equal = PlanScan(coordinator_database, coordinator, not_equal_sql, 8, not_equal_baseline_result->types,
	                          not_equal_baseline_result->names);
	ValidateSplitContract(not_equal.splits, 2);
	Check(not_equal.splits.size() == 2 && not_equal.splits[0].split_id == "0" && not_equal.splits[1].split_id == "2",
	      "file_index-not-equal predicate planned the wrong split set");
	vector<ResultRow> not_equal_rows;
	for (idx_t split_index = 0; split_index < not_equal.splits.size(); split_index++) {
		auto rows =
		    ExecuteAssigned(not_equal, worker, MakeSplitBatch(not_equal.splits[split_index]), 260 + split_index);
		not_equal_rows.insert(not_equal_rows.end(), rows.begin(), rows.end());
	}
	std::sort(not_equal_rows.begin(), not_equal_rows.end());
	Check(not_equal_rows == not_equal_baseline_rows, "file_index-not-equal distributed result differs from baseline");
	ExpectApplyFailure(not_equal, worker, MakeSplitBatch(planned.splits[1]), "outside the planned file set");

	const auto row_number_sql =
	    "SELECT id, file_row_number FROM read_vortex(" + file_list + ") WHERE file_row_number != 0";
	auto row_number_result = coordinator.Query(row_number_sql);
	Check(row_number_result != nullptr && !row_number_result->HasError(), "file_row_number fallback filter failed");
	Check(ReadRows(*row_number_result).size() == 27, "file_row_number fallback filter returned the wrong row count");

	// If coordinator pruning removes every file, Vane still transports one
	// legal explicit empty extension split.
	const auto no_match_sql = "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE file_index = 99";
	auto no_match_baseline_result = coordinator.Query(no_match_sql);
	Check(no_match_baseline_result != nullptr && !no_match_baseline_result->HasError(),
	      "empty-split local Vortex baseline failed");
	Check(ReadRows(*no_match_baseline_result).empty(), "empty-split local baseline returned rows");
	auto no_match = PlanScan(coordinator_database, coordinator, no_match_sql, 8, no_match_baseline_result->types,
	                         no_match_baseline_result->names);
	ValidateSplitContract(no_match.splits, 0);
	Check(ExecuteAssigned(no_match, worker, MakeSplitBatch(no_match.splits[0]), 275).empty(),
	      "fully pruned Vortex split scanned data");

	// Planning always emits elementary singleton splits. The scheduler may merge
	// multiple compatible splits into one worker batch.
	auto merged = PlanScan(coordinator_database, coordinator, scan_sql, 1, baseline_types, baseline_names);
	ValidateSplitContract(merged.splits, files.size());
	auto merged_batch = MakeSplitBatch(merged.splits);
	Check(ExecuteAssigned(merged, worker, merged_batch, 300) == baseline_rows,
	      "merged Vortex split result differs from baseline");
	auto reordered_merged = merged_batch;
	std::reverse(reordered_merged.splits.begin(), reordered_merged.splits.end());
	auto reordered_plan = ClonePlan(merged.worker_plan, worker, 301);
	ApplySplitBatch(*reordered_plan, 301, merged_batch);
	ApplySplitBatch(*reordered_plan, 301, reordered_merged);
	auto reordered_result = ExecutePlan(worker, std::move(reordered_plan), baseline_types, baseline_names,
	                                    "test:vortex-distributed-reordered-assignment");
	Check(reordered_result != nullptr && ReadRows(*reordered_result) == baseline_rows,
	      "reordering a merged Vortex assignment changed its meaning");

	// Vortex removes the upper aggregate operator when all aggregates are
	// pushed into its scan. Such a scan must stay one elementary split containing
	// the complete bound file set; otherwise workers would emit unmergeable
	// per-file final aggregates.
	const auto aggregate_sql = "SELECT min(id), max(id), count(id) FROM read_vortex(" + file_list + ")";
	auto aggregate_baseline_result = coordinator.Query(aggregate_sql);
	Check(aggregate_baseline_result != nullptr && !aggregate_baseline_result->HasError(),
	      "aggregate-pushed local Vortex baseline failed");
	auto aggregate_baseline = ReadAggregateRow(*aggregate_baseline_result);
	Check(aggregate_baseline == AggregateRow {0, 29, 30}, "unexpected aggregate-pushed local baseline");
	auto aggregate_plan = PlanScan(coordinator_database, coordinator, aggregate_sql, 8,
	                               aggregate_baseline_result->types, aggregate_baseline_result->names);
	ValidateSplitContract(aggregate_plan.splits, 1);
	Check(aggregate_plan.splits.size() == 1, "aggregate-pushed Vortex scan was split across workers");
	Check(aggregate_plan.splits[0].split_id == "0,1,2",
	      "aggregate-pushed Vortex split did not retain its complete stable file set");
	Check(ExecuteAggregateAssigned(aggregate_plan, worker, MakeSplitBatch(aggregate_plan.splits[0]), 325) ==
	          aggregate_baseline,
	      "aggregate-pushed distributed Vortex result differs from baseline");
	// A pushed aggregate is indivisible: accepting a valid single-file split in
	// place of the complete planned file-set split would silently return a partial
	// final aggregate.
	ExpectApplyFailure(aggregate_plan, worker, first_batch, "complete planned file set");
	auto empty_aggregate_batch = MakeEmptySplitBatch(aggregate_plan.splits.front());
	Check(ExecuteAssignedRowCount(aggregate_plan, worker, empty_aggregate_batch, 326) == 0,
	      "empty aggregate Vortex split emitted an aggregate row");

	// A complex filter can live exclusively in the portable Rust bind while the
	// aggregate above it is also replaced by the Vortex scan. Exercise both
	// pieces of state together, not only in independent queries.
	const auto filtered_pushed_aggregate_sql =
	    "SELECT min(id), max(id), count(id) FROM read_vortex(" + file_list + ") WHERE payload LIKE '%2%'";
	auto filtered_pushed_aggregate_result = coordinator.Query(filtered_pushed_aggregate_sql);
	Check(filtered_pushed_aggregate_result != nullptr && !filtered_pushed_aggregate_result->HasError(),
	      "complex-filter aggregate-pushed local baseline failed");
	auto filtered_pushed_aggregate_baseline = ReadAggregateRow(*filtered_pushed_aggregate_result);
	Check(filtered_pushed_aggregate_baseline == AggregateRow {2, 29, 12},
	      "unexpected complex-filter aggregate-pushed baseline");
	auto filtered_pushed_aggregate_plan =
	    PlanScan(coordinator_database, coordinator, filtered_pushed_aggregate_sql, 8,
	             filtered_pushed_aggregate_result->types, filtered_pushed_aggregate_result->names);
	ValidateSplitContract(filtered_pushed_aggregate_plan.splits, 1);
	Check(filtered_pushed_aggregate_plan.splits.size() == 1 &&
	          filtered_pushed_aggregate_plan.splits[0].split_id == "0,1,2",
	      "complex-filter aggregate was not preserved as one complete Vortex split");
	Check(ExecuteAggregateAssigned(filtered_pushed_aggregate_plan, worker,
	                               MakeSplitBatch(filtered_pushed_aggregate_plan.splits[0]),
	                               327) == filtered_pushed_aggregate_baseline,
	      "complex-filter aggregate-pushed distributed Vortex result differs from baseline");

	// Aggregate pushdown cannot preserve DuckDB table-filter column identities:
	// the rewrite replaces the scan column IDs with aggregate output positions.
	// Keep a filtered aggregate above the scan, including for virtual filters,
	// so a fully pruned scan still produces the correct SQL aggregate identity.
	const auto empty_aggregate_sql =
	    "SELECT min(id), max(id), count(id) FROM read_vortex(" + file_list + ") WHERE file_index = 99";
	auto empty_aggregate_baseline_result = coordinator.Query(empty_aggregate_sql);
	if (!empty_aggregate_baseline_result || empty_aggregate_baseline_result->HasError()) {
		throw std::runtime_error(
		    "fully pruned aggregate local baseline failed: " +
		    (empty_aggregate_baseline_result ? empty_aggregate_baseline_result->GetError() : "null result"));
	}
	CheckEmptyAggregateRow(*empty_aggregate_baseline_result);

	// A bound file with an empty logical table remains a normal, retryable
	// elementary split whose execution returns zero rows.
	auto empty_file = temp.path / "empty.vortex";
	RequireQuerySuccess(
	    coordinator, "COPY (SELECT CAST(range AS BIGINT) AS id, CAST(NULL AS VARCHAR) AS payload FROM range(0)) TO " +
	                     SQLString(empty_file) + " (FORMAT VORTEX)");
	const auto empty_file_sql = "SELECT id, file_index FROM read_vortex(" + SQLString(empty_file) + ")";
	auto empty_file_baseline = coordinator.Query(empty_file_sql);
	Check(empty_file_baseline != nullptr && !empty_file_baseline->HasError(), "empty Vortex file baseline failed");
	Check(ReadRows(*empty_file_baseline).empty(), "empty Vortex file returned local rows");
	auto empty_file_plan = PlanScan(coordinator_database, coordinator, empty_file_sql, 4, empty_file_baseline->types,
	                                empty_file_baseline->names);
	ValidateSplitContract(empty_file_plan.splits, 1, "read_vortex", LogicalType::VARCHAR);
	Check(ExecuteAssigned(empty_file_plan, worker, MakeSplitBatch(empty_file_plan.splits[0]), 350).empty(),
	      "empty Vortex file returned distributed rows");

	// An applied empty extension split is a legal zero-row scan.
	auto empty_batch = MakeEmptySplitBatch(planned.splits.front());
	auto empty_rows = ExecuteAssigned(planned, worker, empty_batch, 400);
	Check(empty_rows.empty(), "empty Vortex split scanned data");

	// A detached worker plan must fail closed instead of falling back to the
	// coordinator's complete file set.
	auto detached = ClonePlan(planned.worker_plan, worker, 500);
	string detached_error;
	Check(!distributed::ValidateDistributedScanSplitsApplied(*detached, &detached_error),
	      "unassigned detached Vortex plan validated");
	auto detached_result =
	    ExecutePlan(worker, std::move(detached), baseline_types, baseline_names, "test:vortex-distributed-detached");
	Check(detached_result != nullptr && detached_result->HasError(), "detached Vortex plan executed successfully");
	Check(StringUtil::Contains(detached_result->GetError(), "explicit split assignment"),
	      "detached Vortex plan failed for the wrong reason: " + detached_result->GetError());

	const auto &valid_split = planned.splits.front();
	auto valid_batch = MakeSplitBatch(valid_split);

	auto duplicate = valid_batch;
	duplicate.splits.push_back(duplicate.splits.front());
	ExpectApplyFailure(planned, worker, duplicate, "appears more than once");

	auto invalid_id = valid_batch;
	invalid_id.splits[0].split_id = "00";
	ExpectApplyFailure(planned, worker, invalid_id, "Invalid distributed Vortex split id");
	auto empty_id = valid_batch;
	empty_id.splits[0].split_id.clear();
	ExpectApplyFailure(planned, worker, empty_id, "split_id");

	auto empty_payload = valid_batch;
	empty_payload.splits[0].extension_payload.clear();
	ExpectApplyFailure(planned, worker, empty_payload, "empty payload");

	auto corrupt_payload = valid_batch;
	corrupt_payload.splits[0].extension_payload[0] = 'X';
	ExpectApplyFailure(planned, worker, corrupt_payload, "payload magic");

	auto invalid_size = valid_batch;
	Check(invalid_size.splits[0].extension_payload.size() >= 9, "Vortex split payload is too short for a file size");
	for (idx_t byte_index = invalid_size.splits[0].extension_payload.size() - 8;
	     byte_index < invalid_size.splits[0].extension_payload.size(); byte_index++) {
		invalid_size.splits[0].extension_payload[byte_index] = static_cast<char>(0xff);
	}
	ExpectApplyFailure(planned, worker, invalid_size, "invalid file size");

	auto wrong_codec = valid_batch;
	wrong_codec.splits[0].split_codec.version++;
	ExpectApplyFailure(planned, worker, wrong_codec, "split codec mismatch");

	auto unknown_index = valid_batch;
	unknown_index.splits[0].split_id = "127";
	unknown_index.splits[0].extension_payload[13] = static_cast<char>(127);
	for (idx_t byte_index = 14; byte_index < 21; byte_index++) {
		unknown_index.splits[0].extension_payload[byte_index] = 0;
	}
	ExpectApplyFailure(planned, worker, unknown_index, "unknown file index");

	auto unknown_file = valid_batch;
	auto filename_offset = unknown_file.splits[0].extension_payload.find(files[0].filename().string());
	Check(filename_offset != string::npos, "split payload did not contain its bound filename");
	unknown_file.splits[0].extension_payload[filename_offset] = 'x';
	ExpectApplyFailure(planned, worker, unknown_file, "does not match the bound file identity");

	// Repeat the same split from a fresh detached clone to model a worker
	// retry. The assignment remains stable and idempotent.
	auto first_retry = ExecuteAssigned(planned, worker, valid_batch, 600);
	auto second_retry = ExecuteAssigned(planned, worker, valid_batch, 601);
	Check(first_retry == second_retry, "repeated Vortex split changed meaning");
}

} // namespace

int main() {
	try {
		TestWriteProtocol();
		TestProtocol();
		std::cout << "Vortex distributed scan and write protocol tests passed" << std::endl;
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "Vortex distributed scan/write protocol test failed: " << error.what() << std::endl;
		return 1;
	}
}
