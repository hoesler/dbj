context("JDBCDriver integration tests")

test_that("column info can be fetched", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)
  res <- dbSendQuery(con, "SELECT * from iris")

  # when
  info <- dbColumnInfo(res)
  
  # then
  expect_that(info, is_a("data.frame"))
})

test_that("data can be fetched", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)
  res <- dbSendQuery(con, "SELECT count(*) FROM iris")
  
  # when
  data <- fetch(res, -1)
  
  # then
  expect_that(data, is_a("data.frame"))
})

test_that("dbHasCompleted is false initially", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  dbWriteTable(con, "iris", iris, overwrite = TRUE)
  res <- dbSendQuery(con, "SELECT * FROM iris")
  
  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_false())
})

test_that("dbHasCompleted is false while fetching", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)
  res <- dbSendQuery(con, "SELECT * FROM iris")
  fetch(res, 1)

  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_false())
})

test_that("dbHasCompleted is true after fetching", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  dbWriteTable(con, "iris", iris, overwrite = TRUE)
  res <- dbSendQuery(con, "SELECT * FROM iris")
  fetch(res)

  # when
  completed <- dbHasCompleted(res)

  # then
  expect_that(completed, is_true())
})

#test_that("dbGetRowCount returns the correct number", {
#  # given
#  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
#  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
#  on.exit(dbDisconnect(con))
#  dbWriteTable(con, "iris", iris, overwrite = TRUE)
#  res <- dbSendQuery(con, "SELECT * FROM iris")
#  data <- fetch(res)
#
#  # when
#  rows <- dbGetRowCount(res)
#
#  # then
#  expect_that(rows,  is_identical_to(nrow(data)))
#})