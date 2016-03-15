mysql_driver_class <- 'com.mysql.jdbc.Driver'
mysql_classpath <- maven_jar('mysql', 'mysql-connector-java', '5.1.38')

DBItest::make_context(
  # DBItest isn't quoting identifiers with dbQuoteIdentifier().
  # So we have to use experimantal feature DATABASE_TO_UPPER=FALSE instead.
  dbj::driver(mysql_driver_class, mysql_classpath),
  list(url = "jdbc:mysql://localhost/dbi_test?useSSL=false&generateSimpleParameterMetadata=true", user = 'dbi_test', password = 'dbi_test'),
  tweaks = DBItest::tweaks(constructor_name = "driver")
)

DBItest::test_all(skip = c(
  # Driver independent
  dbj_skips_global,

  # MySQL specific
  "get_query_empty_.*",                         # syntax not supported
  "data_logical",                               # not an error: no logical data type
  "data_logical_null_.*",                       # not an error: no logical data type
  "data_logical_int",                           # not an error: no logical data type
  "data_logical_int_null_.*",                   # not an error: no logical data type
  "data_raw",                                   # not an error: can't cast to blob type
  "data_raw_null_.*",                           # not an error: can't cast to blob type
  "data_timestamp_utc",                         # syntax not supported
  "data_timestamp_utc_null_.*",                 # syntax not supported
  "data_timestamp_parens",                      # syntax not supported
  "data_timestamp_parens_null_.*",              # syntax not supported
  "roundtrip_logical",                          # not an error: no logical data type

  "data_integer($|_.+)",                        # Arithmetic operations are returned as BIGINT
  "data_date($|_.+)",                           # TODO
  NULL
))

test_bdj_extras(skip = c(),
  default_driver_args = list(driverClass = mysql_driver_class, classPath = mysql_classpath)
)
