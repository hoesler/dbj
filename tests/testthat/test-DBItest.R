test_h2 <- function(ctx) {
	DBItest::test_getting_started(ctx = ctx, skip = c(
		"package_name", # too restrictive
		"package_dependencies" # 'methods' must be listed in 'Depends' for R <= 3.1.1
	))
	DBItest::test_driver(skip = c(
		"constructor_strict", "constructor", # too restrictive
		"stress_load_unload" # substitute in make_context does not substitute (getOption("h2_jar") is passed on unevaluated)
	), ctx = ctx)
	DBItest::test_connection(skip = c(
		"stress_load_connect_unload" # substitute in make_context does not substitute (getOption("h2_jar") is passed on unevaluated)
	), ctx = ctx)
	DBItest::test_result(skip = c(
		"get_query_empty_single_column", "get_query_empty_multi_column", # default implementation of dbGetQuery in 0.3.1 returns NULL for empty results
		"stale_result_warning", # Currently I have no simple soulution how to keep track of results
		"data_logical_int($|_.+)", # I don't understand these tests. Why and when should logicals be ints?
		"data_date($|_.+)", # date() is undefined in H2.
		"data_time($|_.+)", # why should time be returned as character?
		"data_timestamp_parens($|_.+)", # datetime is an unsupported function in H2
		"data_timestamp_utc($|_.+)", # If we store timestamp as SQL TIMESTAMP we have to discard timezone information.
		"data_raw($|_.+)", # SELECT cast(1 as BLOB) is invalid sytax in H2
		"data_64_bit($|_.+)", # don't understand the rational behind the test
		"data_type_connection" # What should be the data type for structure(.(value), class = "unknown1")?
	), ctx = ctx)
	DBItest::test_sql(skip = c(
		"quote_string", "quote_identifier_not_vectorized", # tests default implementation in DBI 0.3.1.
		"append_table_error", # append to nonexisting table should only fail if create = FALSE
		"overwrite_table", # similar to truncate
		"temporary_table", # this is a post 0.3.1 DBI feature
		"roundtrip_logical_int", # logicals are currently mapped to BOOLEAN not INT
		"roundtrip_64_bit", # dbWriteTable does not support a field.types argument
		"roundtrip_rownames", # this is a post 0.3.1 DBI feature
		"roundtrip_date", # TODO: storing dates as DATE seems unsafe as there might be conflicts with the timezone. H2 to Java Date is -1h. Seems like H2 is storing the date at local UTC+1 and Date is UTC
		"roundtrip_timestamp" # TODO: How can I differentiate between POSIXct and POSIXlt?
	), ctx = ctx)
	DBItest::test_meta(skip = c(
		"get_exception", # DBI 0.3.1 tells me to return a list not a character
		"column_info", # DBI 0.3.1 tells me to return name, field.type, and data.type not "name" and "type"
		"row_count", # TODO: fix. See comment in function
		"bind($|_.+)" # dbBind is not defined in DBI 0.3.1
	), ctx = ctx)
	DBItest::test_compliance(skip = c(
		"read_only" # Why should writing to the database fail?
	), ctx = ctx)
}

requireOption <- function(x) {
	if (!x %in% names(options())) stop(sprintf("Option %s is not present", x))
	getOption(x)
}

message("Testing H2 context")
h2_ctx <- DBItest::make_context(
	# DBItest isn't quoting identifiers with dbQuoteIdentifier().
	# So we have to use experimantal feature DATABASE_TO_UPPER=FALSE instead.
	JDBC('org.h2.Driver', requireOption("h2_jar")), list(url = "jdbc:h2:mem:dbi-test;DATABASE_TO_UPPER=FALSE", user = 'sa'),
	set_as_default = FALSE
)
test_h2(h2_ctx)
