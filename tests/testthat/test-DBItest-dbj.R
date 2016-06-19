DBItest::make_context(
  dbj::driver(), list(),
  tweaks = DBItest::tweaks(constructor_name = "driver"),
  name = "dbj"
)

DBItest::test_getting_started(dbj_skips_global)
DBItest::test_driver(dbj_skips_global)
DBItest::test_compliance(skip = c(
  dbj_skips_global,
  "read_only"
))