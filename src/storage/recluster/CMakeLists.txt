add_library_unity(
  duckdb_storage_recluster OBJECT checkpoint_snapshot.cpp
  layout_row_group_cursor.cpp range_task.cpp recluster_manager.cpp row_group_layout.cpp table_recluster_state.cpp
  table_sort_bind.cpp table_sort_metadata.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_recluster>
    PARENT_SCOPE)
