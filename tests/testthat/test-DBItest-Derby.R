derby_driver_class <- 'org.apache.derby.jdbc.EmbeddedDriver'
derby_classpath <- resolve(
  module('org.apache.derby:derby:10.12.1.1')
)

DBItest::make_context(
  dbj::driver(derby_driver_class, derby_classpath), list(url = "jdbc:derby:memory:dbi-test;create=true", user = 'sa'),
  tweaks = DBItest::tweaks(constructor_name = "driver"),
  name = "Derby"
)

DBItest::test_all(skip = c(
  # Driver independent
  dbj_skips_global,

  # Derby specific

  "quote_string", # TODO: What is causing an error here?
  "temporary_table", # TODO: Derby uses a special syntax for creating temporary tables: DECLARE GLOBAL TEMPORARY TABLE

  # Not an error: Syntax not supported (WHERE clause is mandatory in SELECT expressions) 
  "trivial_query",
  "clear_result_return",
  "command_query",
  "fetch_.*",
  "get_query_.*",
  "data_.*",
  "quote_identifier.*",
  "is_valid_result",
  "get_statement",
  "row_count",
  "get_info_result",

  # Not an error: DBItest does not quote identifiers in select expressions. https://github.com/rstats-db/DBItest/issues/64
  "write_table",
  "read_table",
  "table_visible_in_other_connection",
  "roundtrip_numeric_special",
  "rows_affected",

  NULL
))

test_bdj_extras(skip = c(),
  default_driver_args = list(driverClass = derby_driver_class, classPath = derby_classpath)
)
