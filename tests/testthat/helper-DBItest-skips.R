# Driver independent DBItest skips
dbj_skips_global <- c(
  "package_name",                         # too restrictive
  "package_dependencies",                 # 'methods' must be listed in 'Depends' for R <= 3.1.1
  "constructor_strict", "constructor",    # too restrictive
  "stress_load.*",                        # substitute in make_context does not substitute   
  "stale_result_warning",                 # Currently I have no simple soulution how to keep track of results
  "data_logical_int($|_.+)",              # I don't understand these tests. Why and when should logicals be ints?
  "data_time($|_.+)",                     # why should time be returned as character?
  "data_timestamp_utc($|_.+)",            # If we store timestamp as SQL TIMESTAMP we have to discard timezone information.
  "data_64_bit($|_.+)",                   # don't understand the rational behind the test
  "data_type_connection",                 # What should be the data type for structure(.(value), class = "unknown1")?
  "append_table_error",                   # append to nonexisting table should only fail if create = FALSE
  "overwrite_table",                      # similar to truncate
  "roundtrip_logical_int",                # logicals are currently mapped to BOOLEAN not INT
  "roundtrip_64_bit",                     # dbWriteTable does not support a field.types argument
  "roundtrip_timestamp",                  # TODO: How can I differentiate between POSIXct and POSIXlt?
  "read_only",                            # TODO: Create read only test context for this test

  # The following tests dont't apply to DBI 0.3.1 
  "column_info",                          # DBI 0.3.1 tells me to return name, field.type, and data.type not "name" and "type"
  "bind($|_.+)",                          # dbBind is not defined in DBI 0.3.1
  "roundtrip_rownames",                   # this is a post 0.3.1 DBI feature
  "temporary_table",                      # this is a post 0.3.1 DBI feature
  "quote_string",                         # NA is not converted to NULL in DBI 0.3.1.
  "quote_identifier_not_vectorized",      # dbQuoteIdentifier does not use encodeString in DBI 0.3.1
  "get_query_empty_(single|multi)_column",# default implementation of dbGetQuery in DBI 0.3.1 returns NULL for empty results 

  NULL
)