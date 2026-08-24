//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/replacement_manifest.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class Deserializer;
class ReadStream;
class Serializer;
class WriteStream;

static constexpr uint32_t REPLACEMENT_MANIFEST_FORMAT_VERSION = 1;

struct ReplacementManifestHeader {
	uint32_t format_version = REPLACEMENT_MANIFEST_FORMAT_VERSION;
	recluster_task_id_t task_id = hugeint_t(0, 0);
	persistent_table_id_t table_id = hugeint_t(0, 0);
	layout_version_t prepared_layout_version = INITIAL_LAYOUT_VERSION;
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	sort_run_id_t run_id = INVALID_SORT_RUN_ID;
	RowGroupRange input_range {0, 0};
	delete_sequence_t last_applied_delete_sequence = 0;
	uint64_t manifest_revision = 1;
};

struct ManifestColumnDefinition {
	persistent_column_id_t column_id = 0;
	LogicalType type;

	void Serialize(Serializer &serializer) const;
	static ManifestColumnDefinition Deserialize(Deserializer &deserializer);
	bool operator==(const ManifestColumnDefinition &other) const;
};

struct ReplacementManifest {
	ReplacementManifestHeader header;
	vector<SortColumnDefinition> sort_columns;
	vector<ManifestColumnDefinition> physical_columns;
	vector<RowGroupPhysicalIdentity> old_groups;
	vector<RowGroupPointer> replacement_groups;
	vector<block_id_t> all_referenced_blocks;
	uint64_t payload_size = 0;
	uint64_t checksum = 0;

	void Seal();
	void VerifySeal() const;
	void Write(WriteStream &stream) const;
	static ReplacementManifest Read(ReadStream &stream);
	void Validate() const;

private:
	vector<uint8_t> SerializePayload() const;
	static ReplacementManifest DeserializePayload(data_ptr_t payload, idx_t payload_size);
};

} // namespace duckdb
