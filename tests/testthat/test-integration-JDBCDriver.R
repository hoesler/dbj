context("JDBCDriver integration tests")

test_that("JDBC() creates a driver object", {
  # when
  drv <- JDBC('org.h2.Driver', getOption("h2_jar"))
  
  # then
  expect_that(drv, is_a("JDBCDriver"))
})

test_that("dbConnect() establishes a connection", {
  # given
  h2_drv <- JDBC('org.h2.Driver', getOption("h2_jar"))
  
  # when
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # then
  expect_that(con, is_a("JDBCConnection"))
})

test_that("dbGetInfo() returns a list", {
  # given
  h2_drv <- JDBC('org.h2.Driver', getOption("h2_jar"))
  
  # when
  info <- dbGetInfo(h2_drv)

  # then
  expect_that(info, is_a("list"))
  expect_that(info$minor_version, is_a("integer"))
  expect_that(info$major_version, is_a("integer"))
})

test_that("summary() prints something", {
  # given
  h2_drv <- JDBC('org.h2.Driver', getOption("h2_jar"))

  # then
  expect_that(summary(h2_drv), prints_text(".*"))
})