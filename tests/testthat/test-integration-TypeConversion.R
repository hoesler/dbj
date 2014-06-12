context("JDBCTypeConversion")

test_that("the default mapping can be overwritten", {
  # given
  mapping <- c(list(
    rjdbc_mapping(
      JDBC_SQL_TYPES$DOUBLE,
      function(x) x + 1,
      function(x) x + 1,
      is.numeric,
      "DOUBLE"
    )
  ), default_rjdbc_mapping)

  # when
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar', mapping = mapping)
  
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
  mapping <- c(list(
    rjdbc_mapping(
      JDBC_SQL_TYPES$DOUBLE,
      function(x) duration(x),
      function(x) as.numeric(x),
      is.duration,
      "DOUBLE"
    )
  ), default_rjdbc_mapping)

  # when
  h2_drv <- JDBC('org.h2.Driver', '../h2.jar',
    mapping = mapping)
  
  # then
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  df <- data.frame(duration = duration(c(0, 10000000, 1/1000)))

  dbWriteTable(con, "time_table", df)
  df_read <- dbReadTable(con, "time_table")

  expect_that(df_read, equals(df))
})