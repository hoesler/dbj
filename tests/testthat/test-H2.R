
test_that("a driver can be created", {
  # when
  drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  
  # then
  expect_that(drv, is_a("JDBCDriver"))
})

test_that("a connection can get established", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  
  # when
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # then
  expect_that(con, is_a("JDBCConnection"))
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

test_that("column info can be fetched", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris, overwrite = TRUE)

  # when
  info <- dbColumnInfo(dbSendQuery(con, "SELECT * from iris"))
  
  # then
  expect_that(info, is_a("data.frame"))
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

test_that("the database can be updated", {
  # given
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', '"')
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  
  # when
  success <- dbSendUpdate(con, "CREATE TABLE foo (alpha VARCHAR(32), beta INT)")
  
  # then
  expect_that(success, is_true())
})