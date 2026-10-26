// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "duckdb.h"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

void Require(bool valid, const std::string &message) {
	if (!valid) {
		throw std::runtime_error(message);
	}
}

double Elapsed(Clock::time_point start) {
	return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

void Phase(const std::string &phase) {
	static const bool enabled = [] {
		auto value = std::getenv("SIFT_BENCH_PHASE_BARRIER");
		return value && std::string(value) == "1";
	}();
	if (!enabled) {
		return;
	}
#if defined(__linux__)
	std::cout << "SIFT_BENCH_PHASE " << getpid() << ' ' << phase << std::endl;
	std::string reply;
	Require(bool(std::getline(std::cin, reply)) && reply == "continue", "Resource phase controller disconnected");
#else
	throw std::runtime_error("Resource phase diagnostics require Linux");
#endif
}

struct Result {
	duckdb_result value {};
	~Result() {
		duckdb_destroy_result(&value);
	}
};

struct Chunk {
	duckdb_data_chunk value = nullptr;
	~Chunk() {
		duckdb_destroy_data_chunk(&value);
	}
};

struct Type {
	duckdb_logical_type value = nullptr;
	~Type() {
		duckdb_destroy_logical_type(&value);
	}
};

struct QueryValue {
	std::vector<duckdb_value> scalars;
	duckdb_value value = nullptr;
	~QueryValue() {
		duckdb_destroy_value(&value);
		for (auto &scalar : scalars) {
			duckdb_destroy_value(&scalar);
		}
	}
};

struct Session {
	duckdb_database database = nullptr;
	duckdb_connection connection = nullptr;
	duckdb_prepared_statement prepared = nullptr;

	void Close() {
		duckdb_destroy_prepare(&prepared);
		duckdb_disconnect(&connection);
		duckdb_close(&database);
	}

	~Session() {
		Close();
	}

	void Execute(const char *sql) {
		Result result;
		if (duckdb_query(connection, sql, &result.value) != DuckDBSuccess) {
			auto error = duckdb_result_error(&result.value);
			throw std::runtime_error(error ? error : "Query failed");
		}
	}
};

uint32_t ReadU32(std::ifstream &input) {
	unsigned char bytes[4];
	Require(bool(input.read(reinterpret_cast<char *>(bytes), 4)), "Truncated query header");
	return uint32_t(bytes[0]) | uint32_t(bytes[1]) << 8 | uint32_t(bytes[2]) << 16 | uint32_t(bytes[3]) << 24;
}

size_t Number(const char *text) {
	std::string value(text);
	Require(!value.empty() && value.find_first_not_of("0123456789") == std::string::npos, "Invalid numeric option");
	return std::stoull(value);
}

struct Hit {
	uint64_t id;
	float distance;
	std::vector<float> embedding;
};

std::vector<Hit> Materialize(duckdb_result &result, size_t dimension, size_t k) {
	std::vector<Hit> hits;
	hits.reserve(k);
	for (idx_t chunk_id = 0; chunk_id < duckdb_result_chunk_count(result); ++chunk_id) {
		Chunk chunk {duckdb_result_get_chunk(result, chunk_id)};
		Require(chunk.value && duckdb_data_chunk_get_column_count(chunk.value) == 3, "Expected three result columns");
		auto ids = duckdb_data_chunk_get_vector(chunk.value, 0);
		auto vectors = duckdb_data_chunk_get_vector(chunk.value, 1);
		auto distances = duckdb_data_chunk_get_vector(chunk.value, 2);
		Type id_type {duckdb_vector_get_column_type(ids)};
		Type vector_type {duckdb_vector_get_column_type(vectors)};
		Type distance_type {duckdb_vector_get_column_type(distances)};
		Require(duckdb_get_type_id(id_type.value) == DUCKDB_TYPE_UBIGINT &&
		            duckdb_get_type_id(distance_type.value) == DUCKDB_TYPE_FLOAT &&
		            duckdb_get_type_id(vector_type.value) == DUCKDB_TYPE_ARRAY &&
		            duckdb_array_type_array_size(vector_type.value) == dimension,
		        "Expected UBIGINT ID, FLOAT array embedding, FLOAT distance");
		auto child = duckdb_array_vector_get_child(vectors);
		Type child_type {duckdb_vector_get_column_type(child)};
		Require(duckdb_get_type_id(child_type.value) == DUCKDB_TYPE_FLOAT, "Expected Float32 embeddings");
		auto id_data = static_cast<const uint64_t *>(duckdb_vector_get_data(ids));
		auto vector_data = static_cast<const float *>(duckdb_vector_get_data(child));
		auto distance_data = static_cast<const float *>(duckdb_vector_get_data(distances));
		auto size = duckdb_data_chunk_get_size(chunk.value);
		Require(size <= k - hits.size(), "Too many returned records");
		for (idx_t row = 0; row < size; ++row) {
			Require(duckdb_validity_row_is_valid(duckdb_vector_get_validity(ids), row) &&
			            duckdb_validity_row_is_valid(duckdb_vector_get_validity(vectors), row) &&
			            duckdb_validity_row_is_valid(duckdb_vector_get_validity(distances), row),
			        "Null returned record");
			Hit hit {id_data[row], distance_data[row], std::vector<float>(dimension)};
			for (size_t component = 0; component < dimension; ++component) {
				Require(duckdb_validity_row_is_valid(duckdb_vector_get_validity(child), row * dimension + component),
				        "Null vector component");
			}
			std::memcpy(hit.embedding.data(), vector_data + row * dimension, dimension * sizeof(float));
			hits.push_back(std::move(hit));
		}
	}
	Require(hits.size() == k, "Missing returned records");
	return hits;
}

void Lifecycle(std::ofstream &output, const char *event, double elapsed, size_t dimension) {
	output << event << ",,,," << elapsed;
	for (size_t column = 0; column < dimension + 3; ++column) {
		output << ',';
	}
	output << '\n';
}

} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 7,
		        "Usage: bench_index_sift_capi <query.sql> <queries.f32bin> <samples.csv> <warmup> <rounds> <k>");
		const auto warmup = Number(argv[4]);
		const auto rounds = Number(argv[5]);
		const auto k = Number(argv[6]);
		Require(warmup >= 1 && warmup <= 100 && rounds >= 1 && rounds <= 100 && k >= 1 && k <= 1000,
		        "Invalid rounds/k");
		std::ifstream input(argv[2], std::ios::binary);
		auto count = ReadU32(input);
		auto dimension = ReadU32(input);
		Require(count >= 1 && count <= 1000 && dimension >= 1 && dimension <= 4096, "Invalid query shape");
		std::vector<std::vector<float>> queries(count, std::vector<float>(dimension));
		for (auto &query : queries) {
			for (auto &value : query) {
				auto bits = ReadU32(input);
				std::memcpy(&value, &bits, sizeof(value));
				Require(std::isfinite(value), "Nonfinite query vector");
			}
		}
		Require(input.peek() == std::char_traits<char>::eof(), "Trailing query data");
		std::ifstream sql_file(argv[1]);
		Require(bool(sql_file), "Cannot read SQL");
		std::stringstream sql;
		sql << sql_file.rdbuf();
		Require(!std::filesystem::exists(argv[3]), "Output already exists");
		std::ofstream output(argv[3]);
		Require(bool(output), "Cannot create output");
		output << std::setprecision(std::numeric_limits<double>::max_digits10);
		output << "event,round,query,warmup,latency_ms,rank,id,distance";
		for (size_t component = 0; component < dimension; ++component) {
			output << ",v" << component;
		}
		output << '\n';
		Session session;
		Phase("before_open");
		auto start = Clock::now();
		Require(duckdb_open(nullptr, &session.database) == DuckDBSuccess, "Cannot open database");
		Require(duckdb_connect(session.database, &session.connection) == DuckDBSuccess, "Cannot connect");
		session.Execute("SET autoload_known_extensions=false; SET autoinstall_known_extensions=false; SET threads=1; "
		                "PRAGMA disable_profiling;");
		Lifecycle(output, "open", Elapsed(start), dimension);
		start = Clock::now();
		if (duckdb_prepare(session.connection, sql.str().c_str(), &session.prepared) != DuckDBSuccess) {
			auto error = duckdb_prepare_error(session.prepared);
			throw std::runtime_error(error ? error : "Prepare failed");
		}
		Require(duckdb_nparams(session.prepared) == 1, "Expected exactly one vector parameter");
		Lifecycle(output, "prepare", Elapsed(start), dimension);
		std::cout << "duckdb_version=" << duckdb_library_version() << '\n';
		Type float_type {duckdb_create_logical_type(DUCKDB_TYPE_FLOAT)};
		Phase("ready");
		for (size_t round = 0; round < warmup + rounds; ++round) {
			Phase("before_round_" + std::to_string(round));
			for (size_t query_id = 0; query_id < queries.size(); ++query_id) {
				QueryValue parameter;
				parameter.scalars.reserve(dimension);
				for (auto value : queries[query_id]) {
					parameter.scalars.push_back(duckdb_create_float(value));
				}
				parameter.value = duckdb_create_array_value(float_type.value, parameter.scalars.data(), dimension);
				Require(parameter.value && duckdb_clear_bindings(session.prepared) == DuckDBSuccess &&
				            duckdb_bind_value(session.prepared, 1, parameter.value) == DuckDBSuccess,
				        "Cannot bind vector");
				Result result;
				if (round == 0 && query_id == 0) {
					Phase("before_first_search");
				}
				start = Clock::now();
				if (duckdb_execute_prepared(session.prepared, &result.value) != DuckDBSuccess) {
					auto error = duckdb_result_error(&result.value);
					throw std::runtime_error(error ? error : "Execute failed");
				}
				auto hits = Materialize(result.value, dimension, k);
				auto elapsed = Elapsed(start);
				if (round == 0 && query_id == 0) {
					Phase("after_first_search");
				}
				for (size_t rank = 0; rank < hits.size(); ++rank) {
					const auto &hit = hits[rank];
					output << "sample," << round << ',' << query_id << ',' << (round < warmup) << ',' << elapsed << ','
					       << rank << ',' << hit.id << ',' << hit.distance;
					for (auto value : hit.embedding) {
						Require(std::isfinite(value), "Nonfinite returned vector");
						output << ',' << value;
					}
					output << '\n';
				}
			}
			output.flush();
			Require(bool(output), "Cannot write samples");
			std::cout << "round=" << round << " warmup=" << (round < warmup) << " completed" << std::endl;
			Phase("after_round_" + std::to_string(round));
		}
		start = Clock::now();
		session.Close();
		Lifecycle(output, "close", Elapsed(start), dimension);
		output.flush();
		Require(bool(output), "Cannot write lifecycle");
		Phase("after_close");
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
