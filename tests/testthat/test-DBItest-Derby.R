derby_driver_class <- 'org.apache.derby.jdbc.EmbeddedDriver'
derby_classpath <- maven_jar('org.apache.derby', 'derby', '10.12.1.1')

DBItest::make_context(
  dbj::driver(derby_driver_class, derby_classpath), list(url = "jdbc:derby:memory:dbi-test;create=true", user = 'sa'),
  tweaks = DBItest::tweaks(constructor_name = "driver")
)

DBItest::test_all(skip = c(
  # Driver independent
  dbj_skips_global,

  # Derby specific

  # No error: Syntax not supported (WHERE clause is mandatory) 
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

  NULL
))

test_bdj_extras(skip = c(),
  default_driver_args = list(driverClass = derby_driver_class, classPath = derby_classpath)
)
