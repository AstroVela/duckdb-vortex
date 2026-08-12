// SPDX-FileCopyrightText: 2026 Vortex contributors
// SPDX-License-Identifier: Apache-2.0

#include "vortex_extension.hpp"

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/distributed/pipeline_node/pipeline_node.hpp"
#include "duckdb/execution/distributed/pipeline_node/translator_scan.hpp"
#include "duckdb/execution/distributed/plan/scan_task.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
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

struct PlannedScan {
	distributed::DuckPhysicalPlanRef worker_plan;
	vector<distributed::ScanTaskDescriptor> descriptors;
	vector<LogicalType> result_types;
	vector<string> result_names;
};

void FindTableScans(PhysicalOperator &op, vector<reference<PhysicalTableScan>> &result) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		result.push_back(op.Cast<PhysicalTableScan>());
	}
	for (auto &child : op.children) {
		FindTableScans(child.get(), result);
	}
}

PlannedScan PlanScan(DuckDB &database, Connection &connection, const string &sql, idx_t worker_slots,
                     const vector<LogicalType> &result_types, const vector<string> &result_names) {
	auto logical_plan = connection.ExtractPlan(sql);
	Check(logical_plan != nullptr, "failed to extract the Vortex logical plan");
	PhysicalPlanGenerator generator(*connection.context);
	auto generated_plan = generator.Plan(std::move(logical_plan));
	Check(generated_plan != nullptr, "failed to generate the Vortex physical plan");
	auto coordinator_plan = distributed::DuckPhysicalPlanRef(generated_plan.release());
	vector<reference<PhysicalTableScan>> table_scans;
	FindTableScans(coordinator_plan->Root(), table_scans);
	Check(table_scans.size() == 1, "predicate/projection query did not contain exactly one PhysicalTableScan");
	auto &coordinator_scan = table_scans.front().get();
	Check(coordinator_scan.function.name == "read_vortex", "planned the wrong table function");
	Check(coordinator_scan.GetTypes().size() == result_types.size(),
	      "standalone Vortex scan changed the projected column count");
	Check(coordinator_scan.function.HasSerializationCallbacks(), "Vortex bind serde is not registered");
	Check(coordinator_scan.function.HasDistributedScanCallbacks(), "Vortex distributed callbacks are not registered");

	distributed::DuckDBExecutionConfig config;
	config.set_distributed_worker_slots(worker_slots);
	PlannedScan result;
	result.worker_plan = distributed::MakeTableScanPlan(coordinator_scan, connection.context.get());
	auto task_set = distributed::MakeTableScanTasks(coordinator_scan, config, database.instance);
	Check(!task_set.known_empty, "non-empty Vortex scan was planned as known empty");
	result.descriptors = std::move(task_set.tasks);
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

void ApplyDescriptor(PhysicalPlan &plan, idx_t scan_node_id, const distributed::ScanTaskDescriptor &descriptor) {
	unordered_map<idx_t, distributed::ScanTaskDescriptor> assignments;
	assignments.emplace(scan_node_id, descriptor);
	string error;
	Check(distributed::ApplyScanTasksToPlan(plan, assignments, &error), "failed to apply Vortex task: " + error);
	Check(distributed::ValidateDistributedScanTasksApplied(plan, &error),
	      "applied Vortex task did not validate: " + error);
}

unique_ptr<QueryResult> ExecutePlan(Connection &worker, unique_ptr<PhysicalPlan> plan, const vector<LogicalType> &types,
                                    const vector<string> &names, const string &label) {
	auto prepared = make_shared_ptr<PreparedStatementData>(StatementType::SELECT_STATEMENT);
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

vector<ResultRow> ExecuteAssigned(const PlannedScan &planned, Connection &worker,
                                  const distributed::ScanTaskDescriptor &descriptor, idx_t scan_node_id) {
	auto plan = ClonePlan(planned.worker_plan, worker, scan_node_id);
	ApplyDescriptor(*plan, scan_node_id, descriptor);
	auto result = ExecutePlan(worker, std::move(plan), planned.result_types, planned.result_names,
	                          "test:vortex-distributed-assigned");
	Check(result != nullptr, "assigned worker returned no result");
	return ReadRows(*result);
}

AggregateRow ExecuteAggregateAssigned(const PlannedScan &planned, Connection &worker,
                                      const distributed::ScanTaskDescriptor &descriptor, idx_t scan_node_id) {
	auto plan = ClonePlan(planned.worker_plan, worker, scan_node_id);
	ApplyDescriptor(*plan, scan_node_id, descriptor);
	auto result = ExecutePlan(worker, std::move(plan), planned.result_types, planned.result_names,
	                          "test:vortex-distributed-aggregate-assigned");
	Check(result != nullptr, "aggregate worker returned no result");
	return ReadAggregateRow(*result);
}

void RecomputeDescriptorEstimates(distributed::ScanTaskDescriptor &descriptor) {
	descriptor.estimated_cardinality = 0;
	descriptor.estimated_bytes = 0;
	for (const auto &task : descriptor.extension_tasks) {
		if (task.estimated_cardinality.IsValid()) {
			descriptor.estimated_cardinality += task.estimated_cardinality.GetIndex();
		}
		if (task.estimated_bytes.IsValid()) {
			descriptor.estimated_bytes += task.estimated_bytes.GetIndex();
		}
	}
}

void ExpectApplyFailure(const PlannedScan &planned, Connection &worker,
                        const distributed::ScanTaskDescriptor &descriptor, const string &expected) {
	try {
		auto plan = ClonePlan(planned.worker_plan, worker, 900);
		unordered_map<idx_t, distributed::ScanTaskDescriptor> assignments;
		assignments.emplace(900, descriptor);
		string error;
		if (!distributed::ApplyScanTasksToPlan(*plan, assignments, &error)) {
			Check(expected.empty() || StringUtil::Contains(error, expected),
			      "unexpected apply error: " + error + ", expected: " + expected);
			return;
		}
		throw std::runtime_error("invalid Vortex task was accepted");
	} catch (const std::exception &error) {
		Check(expected.empty() || StringUtil::Contains(error.what(), expected),
		      "unexpected apply exception: " + string(error.what()) + ", expected: " + expected);
	}
}

void ValidateDescriptorContract(const vector<distributed::ScanTaskDescriptor> &descriptors, idx_t file_count) {
	Check(!descriptors.empty(), "Vortex planner returned no descriptors");
	set<string> task_ids;
	idx_t elementary_count = 0;
	for (const auto &descriptor : descriptors) {
		Check(descriptor.kind == distributed::ScanTaskKind::EXTENSION, "Vortex emitted a non-extension task");
		Check(descriptor.files.empty(), "Vortex extension task unexpectedly contains file tasks");
		Check(descriptor.extension_capability.extension_name == "vortex", "wrong Vortex capability owner");
		Check(descriptor.extension_capability.capability.name == "read_vortex", "wrong Vortex capability name");
		Check(descriptor.extension_capability.capability.protocol_version == 1, "wrong Vortex protocol version");
		Check(descriptor.task_codec.name == "vane.vortex-file-task", "wrong Vortex task codec");
		Check(descriptor.task_codec.version == 1, "wrong Vortex task codec version");
		for (const auto &task : descriptor.extension_tasks) {
			Check(!task.task_id.empty(), "empty Vortex task id");
			Check(!task.payload.empty(), "empty Vortex task payload");
			Check(task.estimated_cardinality.IsValid(), "missing Vortex task cardinality estimate");
			Check(task.estimated_bytes.IsValid(), "missing Vortex task byte estimate");
			Check(task_ids.insert(task.task_id).second, "duplicate planned Vortex task id");
			elementary_count++;
		}
		auto roundtrip = distributed::ScanTaskDescriptor::DeserializeFromBytes(descriptor.SerializeToBytes());
		Check(roundtrip.extension_tasks.size() == descriptor.extension_tasks.size(),
		      "descriptor round-trip changed the task count");
		Check(roundtrip.task_codec == descriptor.task_codec, "descriptor round-trip changed the codec");
	}
	Check(elementary_count == file_count, "Vortex did not plan exactly one task per bound file");
}

void TestProtocol() {
	auto temp = MakeTempDirectory();
	vector<std::filesystem::path> files;
	for (idx_t file_index = 0; file_index < 3; file_index++) {
		files.push_back(temp.path / ("part-" + std::to_string(file_index) + ".vortex"));
	}

	DuckDB coordinator_database(nullptr);
	coordinator_database.LoadStaticExtension<VortexExtension>();
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
	ValidateDescriptorContract(planned.descriptors, files.size());
	Check(planned.descriptors.size() == files.size(), "workers > tasks changed elementary task count");

	DuckDB worker_database(nullptr);
	worker_database.LoadStaticExtension<VortexExtension>();
	Connection worker(worker_database);

	// Clone while the data directory is unavailable. A hidden call to the
	// original bind would attempt to enumerate these paths and fail. The worker
	// deserialize path must only rebuild owned portable metadata.
	auto hidden_path = temp.path;
	hidden_path += ".hidden";
	std::filesystem::rename(temp.path, hidden_path);
	unique_ptr<PhysicalPlan> lifecycle_plan;
	try {
		lifecycle_plan = ClonePlan(planned.worker_plan, worker, 100);
		std::filesystem::rename(hidden_path, temp.path);
	} catch (...) {
		std::filesystem::rename(hidden_path, temp.path);
		throw;
	}
	ApplyDescriptor(*lifecycle_plan, 100, planned.descriptors.front());
	auto retry_plan_ref = distributed::DuckPhysicalPlanRef(lifecycle_plan.release());
	auto retry_clone = ClonePlan(retry_plan_ref, worker, 100);
	// Physical-plan cloning intentionally resets Vane's runtime-applied marker.
	// Re-applying the same descriptor must be safe; the bind serde still carries
	// the assignment and cannot broaden it while cloning.
	ApplyDescriptor(*retry_clone, 100, planned.descriptors.front());
	auto retry_result = ExecutePlan(worker, std::move(retry_clone), baseline_types, baseline_names,
	                                "test:vortex-distributed-retry-clone");
	Check(retry_result != nullptr && !retry_result->HasError(), "retry clone execution failed");

	vector<ResultRow> distributed_rows;
	for (idx_t descriptor_index = 0; descriptor_index < planned.descriptors.size(); descriptor_index++) {
		auto roundtrip = distributed::ScanTaskDescriptor::DeserializeFromBytes(
		    planned.descriptors[descriptor_index].SerializeToBytes());
		auto rows = ExecuteAssigned(planned, worker, roundtrip, 200 + descriptor_index);
		distributed_rows.insert(distributed_rows.end(), rows.begin(), rows.end());
	}
	std::sort(distributed_rows.begin(), distributed_rows.end());
	Check(distributed_rows == baseline_rows, "per-file worker results do not equal the local Vortex scan");

	// A file_index predicate can be evaluated without opening readers. The
	// coordinator must omit files that cannot match while preserving the stable
	// original file index in the task id and worker output.
	const auto pruned_sql =
	    "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE file_index = 1 AND id >= 12 AND id < 18";
	auto pruned_baseline_result = coordinator.Query(pruned_sql);
	Check(pruned_baseline_result != nullptr && !pruned_baseline_result->HasError(),
	      "file_index-pruned local Vortex baseline failed");
	auto pruned_baseline_rows = ReadRows(*pruned_baseline_result);
	Check(pruned_baseline_rows.size() == 6, "unexpected file_index-pruned baseline cardinality");
	auto pruned = PlanScan(coordinator_database, coordinator, pruned_sql, 8, pruned_baseline_result->types,
	                       pruned_baseline_result->names);
	ValidateDescriptorContract(pruned.descriptors, 1);
	Check(pruned.descriptors.size() == 1 && pruned.descriptors[0].extension_tasks.size() == 1,
	      "file_index predicate did not prune the distributed task set");
	Check(pruned.descriptors[0].extension_tasks[0].task_id == "1",
	      "file_index pruning changed the stable coordinator task id");
	Check(ExecuteAssigned(pruned, worker, pruned.descriptors[0], 250) == pruned_baseline_rows,
	      "file_index-pruned worker result differs from baseline");

	// If coordinator pruning removes every file, Vane still transports one
	// legal extension descriptor with an empty elementary-task array.
	const auto no_match_sql = "SELECT id, file_index FROM read_vortex(" + file_list + ") WHERE file_index = 99";
	auto no_match_baseline_result = coordinator.Query(no_match_sql);
	Check(no_match_baseline_result != nullptr && !no_match_baseline_result->HasError(),
	      "empty-task local Vortex baseline failed");
	Check(ReadRows(*no_match_baseline_result).empty(), "empty-task local baseline returned rows");
	auto no_match = PlanScan(coordinator_database, coordinator, no_match_sql, 8, no_match_baseline_result->types,
	                         no_match_baseline_result->names);
	ValidateDescriptorContract(no_match.descriptors, 0);
	Check(no_match.descriptors.size() == 1 && no_match.descriptors[0].extension_tasks.empty(),
	      "fully pruned Vortex scan did not produce an empty extension descriptor");
	Check(ExecuteAssigned(no_match, worker, no_match.descriptors[0], 275).empty(),
	      "fully pruned Vortex descriptor scanned data");

	// One descriptor may carry multiple elementary tasks.
	auto merged = PlanScan(coordinator_database, coordinator, scan_sql, 1, baseline_types, baseline_names);
	Check(merged.descriptors.size() == 1, "one worker slot did not merge Vortex file tasks");
	Check(merged.descriptors[0].extension_tasks.size() == files.size(),
	      "merged descriptor does not contain every file task");
	Check(ExecuteAssigned(merged, worker, merged.descriptors[0], 300) == baseline_rows,
	      "merged Vortex task result differs from baseline");

	// Vortex removes the upper aggregate operator when all aggregates are
	// pushed into its scan. Such a scan must stay one elementary task containing
	// the complete pruned file set; otherwise workers would emit unmergeable
	// per-file final aggregates.
	const auto aggregate_sql =
	    "SELECT min(id), max(id), count(id) FROM read_vortex(" + file_list + ") WHERE id >= 7 AND id < 25";
	auto aggregate_baseline_result = coordinator.Query(aggregate_sql);
	Check(aggregate_baseline_result != nullptr && !aggregate_baseline_result->HasError(),
	      "aggregate-pushed local Vortex baseline failed");
	auto aggregate_baseline = ReadAggregateRow(*aggregate_baseline_result);
	Check(aggregate_baseline == AggregateRow {7, 24, 18}, "unexpected aggregate-pushed local baseline");
	auto aggregate_plan = PlanScan(coordinator_database, coordinator, aggregate_sql, 8,
	                               aggregate_baseline_result->types, aggregate_baseline_result->names);
	ValidateDescriptorContract(aggregate_plan.descriptors, 1);
	Check(aggregate_plan.descriptors.size() == 1 && aggregate_plan.descriptors[0].extension_tasks.size() == 1,
	      "aggregate-pushed Vortex scan was split across workers");
	Check(aggregate_plan.descriptors[0].extension_tasks[0].task_id == "0,1,2",
	      "aggregate-pushed Vortex task did not retain its complete stable file set");
	Check(ExecuteAggregateAssigned(aggregate_plan, worker, aggregate_plan.descriptors[0], 325) == aggregate_baseline,
	      "aggregate-pushed distributed Vortex result differs from baseline");

	// A bound file with an empty logical table remains a normal, retryable
	// elementary task whose execution returns zero rows.
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
	ValidateDescriptorContract(empty_file_plan.descriptors, 1);
	Check(ExecuteAssigned(empty_file_plan, worker, empty_file_plan.descriptors[0], 350).empty(),
	      "empty Vortex file returned distributed rows");

	// An applied empty extension descriptor is a legal zero-row scan.
	auto empty_descriptor = planned.descriptors.front();
	empty_descriptor.extension_tasks.clear();
	empty_descriptor.estimated_cardinality = 0;
	empty_descriptor.estimated_bytes = 0;
	auto empty_rows = ExecuteAssigned(planned, worker, empty_descriptor, 400);
	Check(empty_rows.empty(), "empty Vortex descriptor scanned data");

	// A detached worker plan must fail closed instead of falling back to the
	// coordinator's complete file set.
	auto detached = ClonePlan(planned.worker_plan, worker, 500);
	string detached_error;
	Check(!distributed::ValidateDistributedScanTasksApplied(*detached, &detached_error),
	      "unassigned detached Vortex plan validated");
	auto detached_result =
	    ExecutePlan(worker, std::move(detached), baseline_types, baseline_names, "test:vortex-distributed-detached");
	Check(detached_result != nullptr && detached_result->HasError(), "detached Vortex plan executed successfully");
	Check(StringUtil::Contains(detached_result->GetError(), "explicit task assignment"),
	      "detached Vortex plan failed for the wrong reason: " + detached_result->GetError());

	const auto &valid_descriptor = planned.descriptors.front();
	Check(valid_descriptor.extension_tasks.size() == 1, "expected an elementary descriptor for negative tests");

	auto duplicate = valid_descriptor;
	duplicate.extension_tasks.push_back(duplicate.extension_tasks.front());
	RecomputeDescriptorEstimates(duplicate);
	ExpectApplyFailure(planned, worker, duplicate, "appears more than once");

	auto invalid_id = valid_descriptor;
	invalid_id.extension_tasks[0].task_id = "00";
	ExpectApplyFailure(planned, worker, invalid_id, "Invalid distributed Vortex task id");

	auto corrupt_payload = valid_descriptor;
	corrupt_payload.extension_tasks[0].payload[0] = 'X';
	ExpectApplyFailure(planned, worker, corrupt_payload, "payload magic");

	auto unknown_index = valid_descriptor;
	unknown_index.extension_tasks[0].task_id = "127";
	unknown_index.extension_tasks[0].payload[13] = static_cast<char>(127);
	for (idx_t byte_index = 14; byte_index < 21; byte_index++) {
		unknown_index.extension_tasks[0].payload[byte_index] = 0;
	}
	ExpectApplyFailure(planned, worker, unknown_index, "unknown file index");

	auto unknown_file = valid_descriptor;
	auto filename_offset = unknown_file.extension_tasks[0].payload.find(files[0].filename().string());
	Check(filename_offset != string::npos, "task payload did not contain its bound filename");
	unknown_file.extension_tasks[0].payload[filename_offset] = 'x';
	ExpectApplyFailure(planned, worker, unknown_file, "does not match the bound file identity");

	// Repeat the same descriptor from a fresh detached clone to model a worker
	// retry. The assignment remains stable and idempotent.
	auto first_retry = ExecuteAssigned(planned, worker, valid_descriptor, 600);
	auto second_retry = ExecuteAssigned(planned, worker, valid_descriptor, 601);
	Check(first_retry == second_retry, "repeated Vortex task changed meaning");
}

} // namespace

int main() {
	try {
		TestProtocol();
		std::cout << "Vortex distributed scan protocol test passed" << std::endl;
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "Vortex distributed scan protocol test failed: " << error.what() << std::endl;
		return 1;
	}
}
