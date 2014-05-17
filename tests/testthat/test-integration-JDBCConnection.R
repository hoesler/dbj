context("JDBCConnection integration tests")

test_that("dbListTables returns all table names", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # when
  tables <- dbListTables(con)
  
  # then
  expect_that(length(tables), equals(29))
})

test_that("a table can be written", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  
  # when
  success <- dbWriteTable(con, "iris", iris, overwrite = TRUE)
  
  # then
  expect_that(success, is_true())
})

test_that("a table can be read", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)

  # when
  data <- dbReadTable(con, "iris")
  
  # then
  expect_that(data, is_a("data.frame"))
  expect_that(ncol(data), equals(ncol(iris)))
  expect_that(nrow(data), equals(nrow(iris)))
})

test_that("a query can be sent", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)

  # when
  res <- dbSendQuery(con, "SELECT count(*) FROM iris")
  
  # then
  expect_that(res, is_a("JDBCResult"))
})

test_that("a prepared query can be sent", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)

  # when
  res <- dbSendQuery(con, "SELECT Species, count(Species) FROM iris WHERE \"Sepal.Width\" > ? GROUP BY Species", 3)
  
  # then
  expect_that(res, is_a("JDBCResult"))
})

test_that("an update query can be sent", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  
  # when
  success <- dbSendUpdate(con, "CREATE TABLE foo (alpha VARCHAR(32), beta INT)")
  
  # then
  expect_that(success, is_true())
})