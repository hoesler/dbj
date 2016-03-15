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
  NULL
))

test_bdj_extras(skip = c(),
  default_driver_args = list(driverClass = sqlite_driver_class, classPath = sqlite_classpath)
)
