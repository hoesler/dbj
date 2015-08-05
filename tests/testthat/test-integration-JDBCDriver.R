context("JDBCDriver integration tests")

test_that("dbj::driver() creates a driver object", {
  # when
  drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  
  # then
  expect_that(drv, is_a("JDBCDriver"))
})

test_that("dbConnect() establishes a connection", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  
  # when
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # then
  expect_that(con, is_a("JDBCConnection"))
})

test_that("dbGetInfo() returns expected data", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))

  # when
  info <- dbGetInfo(h2_drv)

  # then
  expect_that(info, is_a("list"))
  expect_that(c("driver.version", "client.version", "max.connections"), is_contained_in(names(info)))
})