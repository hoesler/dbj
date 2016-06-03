context("SQLDialect")

test_that("We can guess the dialect", {
  # given
  driver_dialect_map <- list(
    'org.h2.Driver' = 'H2',
    'foobar' = 'generic',
    NULL = 'generic'
  )

  lapply(names(driver_dialect_map), function(driver_class) {
    dialect <- guess_dialect(driver_class)
    expect_is(dialect, 'sql_dialect')
    expect_equal(attr(dialect, "name"), driver_dialect_map[[driver_class]])
  })
})
