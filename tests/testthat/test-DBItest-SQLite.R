sqlite_driver_class <- 'org.sqlite.JDBC'
sqlite_classpath <- maven_jar('org.xerial', 'sqlite-jdbc', '3.8.11.2')

DBItest::make_context(
  # DBItest isn't quoting identifiers with dbQuoteIdentifier().
  # So we have to use experimantal feature DATABASE_TO_UPPER=FALSE instead.
  dbj::driver(sqlite_driver_class, sqlite_classpath), list(url = "jdbc:sqlite::memory:"),
  tweaks = DBItest::tweaks(constructor_name = "driver")
)

DBItest::test_all(skip = c(
  # Driver independent
  dbj_skips_global,

  # SQLite specific
  "data_logical",                               # not an error, no logical data type
  "data_logical_null_.*",                       # not an error, no logical data type
  "roundtrip_logical",                          # not an error, no logical data type
  "data_time",                                  # syntax not supported
  "data_time_null_.*",                          # syntax not supported
  "data_timestamp",                             # syntax not supported
  "data_timestamp_null_.*",                     # syntax not supported
  "data_timestamp_utc",                         # syntax not supported
  "data_timestamp_utc_null_.*",                 # syntax not supported

  ".*_null_(above|below)",                      # Driver returns row dependent column type

  NULL
))

test_bdj_extras(skip = c(),
  default_driver_args = list(driverClass = sqlite_driver_class, classPath = sqlite_classpath)
)
