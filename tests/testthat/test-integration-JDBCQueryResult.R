context("JDBCDriver integration tests")

test_that("column info can be fetched", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")

  # when
  info <- dbColumnInfo(res)
  
  # then
  expect_that(info, is_a("data.frame"))
  expect_that(c("name", "field.type", "data.type"), is_contained_in(names(info)))
  expect_that(names(iris), equals(info$name))
  expect_that(c("DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "VARCHAR"), equals(info$field.type))
  expect_that(c("numeric", "numeric", "numeric", "numeric", "character"), equals(info$data.type))
})

test_that("dbListFields returns filed names", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")

  # when
  info <- dbListFields(res)
  
  # then
  expect_that(info, is_a("character"))
  expect_that(names(iris), equals(info))
})

test_that("data can be fetched", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT count(*) FROM \"iris\"")
  
  # when
  data <- fetch(res, -1)
  
  # then
  expect_that(data, is_a("data.frame"))
})

test_that("dbHasCompleted is false initially", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")
  
  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_false())
})

test_that("dbHasCompleted is false while fetching", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")
  fetch(res, 1)

  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_false())
})

test_that("dbHasCompleted is true after fetching", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")
  fetch(res)

  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_true())
})

test_that("dbGetStatement returns the statement", {
  # given
  statement <- 'SELECT 1 + 1'
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  res <- dbSendQuery(con, statement)

  # when
  actual <- dbGetStatement(res)

  # then
  expect_that(actual, equals(statement))
})

test_that("dbGetInfo() returns expected data", {
  # given
  statement <- 'SELECT 1 + 1'
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  res <- dbSendQuery(con, statement)

  # when
  info <- dbGetInfo(res)

  # then
  expect_that(info, is_a("list"))
  expect_that(c("statement", "row.count", "rows.affected", "has.completed", "is.select"), is_contained_in(names(info)))
})

#test_that("dbGetRowCount returns the correct number", {
#  # given
#  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
#  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
#  on.exit(dbDisconnect(con))
#  dbWriteTable(con, "iris", iris)
#  res <- dbSendQuery(con, "SELECT * FROM \"iris\"")
#  data <- fetch(res)
#
#  # when
#  rows <- dbGetRowCount(res)
#
#  # then
#  expect_that(rows,  is_identical_to(nrow(data)))
#})