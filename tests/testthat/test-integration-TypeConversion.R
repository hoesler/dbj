context("JDBCTypeConversion")

test_that("the default mapping can be overwritten", {
  # given
  read_conversions <- c(list(
    sqltype_read_conversion(JDBC_SQL_TYPES$DOUBLE, function(x) x + 1)
  ), default_read_conversions)

  write_conversions <- c(list(
    mapped_write_conversion("numeric", function(x) x + 1, "DOUBLE")
  ), default_write_conversions)

  # when
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', read_conversions = read_conversions, write_conversions = write_conversions)
  
  # then
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  iris_read <- dbReadTable(con, "iris")

  expect_that(iris_read$Sepal.Length, equals(iris$Sepal.Length + 2))
})

test_that("I can map Duration to DOUBLE", {
  # given
  library(lubridate)
  read_conversions <- c(list(
    sqltype_read_conversion(JDBC_SQL_TYPES$DOUBLE, function(x) duration(x))
  ), default_read_conversions)

  write_conversions <- c(list(
    mapped_write_conversion("Duration", function(x) as.numeric(x), "DOUBLE")
  ), default_write_conversions)

  # when
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', read_conversions = read_conversions, write_conversions = write_conversions)
  
  # then
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  df <- data.frame(duration = duration(c(0, 10000000, 1/1000)))

  dbWriteTable(con, "time_table", df)
  df_read <- dbReadTable(con, "time_table")

  expect_that(df_read, equals(df))
})