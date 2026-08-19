add_library_unity(duckdb_storage_recluster OBJECT row_group_layout.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_recluster>
    PARENT_SCOPE)
