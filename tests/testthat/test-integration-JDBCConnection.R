context("JDBCConnection integration tests")

test_that("dbListTables returns all table names", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # when
  tables <- dbListTables(con)
  
  # then
  expect_that(length(tables), equals(29))
})

test_that("dbWriteTable will write iris data.frame", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  
  # when
  success <- dbWriteTable(con, "iris", iris)
  
  # then
  expect_that(success, is_true())
})

test_that("dbWriteTable will write Batting data.frame", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  library(Lahman)
  
  # when
  success <- dbWriteTable(con, "Master", Master)
  
  # then
  expect_that(success, is_true())
})

test_that("a table can be read", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)

  # when
  data <- dbReadTable(con, "iris")
  
  # then
  expect_that(data, is_a("data.frame"))
  expect_that(names(data), equals(names(iris)))
  expect_that(ncol(data), equals(ncol(iris)))
  expect_that(nrow(data), equals(nrow(iris)))
})

test_that("a query can be sent", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)

  # when
  res <- dbSendQuery(con, "SELECT count(*) FROM \"iris\"")
  
  # then
  expect_that(res, is_a("JDBCQueryResult"))
})

test_that("a prepared query can be sent", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)

  # when
  res <- dbSendQuery(con, "SELECT \"Species\", count(\"Species\") FROM \"iris\" WHERE \"Sepal.Width\" > ? GROUP BY \"Species\"", list(3))
  
  # then
  expect_that(res, is_a("JDBCQueryResult"))
})

test_that("an update query can be sent", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  
  # when
  success <- dbSendUpdate(con, "CREATE TABLE \"foo\" (alpha VARCHAR(32), beta INT)")
  
  # then
  expect_that(success, is_true())
})

test_that("SQLKeywords() returns a character vector of keywords", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  
  # when
  keywords <- SQLKeywords(con)
  
  # then
  expect_that(keywords, is_a("character"))
  # H2 keywords (see http://h2database.com/javadoc/org/h2/jdbc/JdbcDatabaseMetaData.html#getSQLKeywords)
  expect_that(all(c("LIMIT","MINUS","ROWNUM","SYSDATE","SYSTIME","SYSTIMESTAMP","TODAY") %in% keywords), is_true())
  expect_that(all(.SQL92Keywords %in% keywords), is_true())
})

test_that("dbGetInfo() returns expected data", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # when
  info <- dbGetInfo(con)

  # then
  expect_that(info, is_a("list"))
  expect_that(c("db.version", "dbname", "username", "host", "port"), is_contained_in(names(info)))
  expect_that(c("password"), not(is_contained_in(names(info))))
})

test_that("dbCallProc() works", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  dbSendUpdate(con, 'CREATE ALIAS NEXT_PRIME AS $$
String nextPrime(String value) {
    return new BigInteger(value).nextProbablePrime().toString();
}
$$;')

  # when
  res <- dbCallProc(con, "NEXT_PRIME", list("12"))
  
  # then
  expect_that(res, is_true())
})

test_that("dbConnect() works", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # when
  con2 <- dbConnect(con)

  # then
  expect_that(con2, is_a("JDBCConnection"))
})

test_that("dbTruncateTable() works using truncate", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  
  # when
  success <- dbTruncateTable(con, "iris", use_delete = FALSE)

  # then
  expect_that(success, is_true())
})

test_that("dbTruncateTable() works using delete from", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))
  data(iris)
  dbWriteTable(con, "iris", iris)
  
  # when
  success <- dbTruncateTable(con, "iris", use_delete = TRUE)

  # then
  expect_that(success, is_true())
})

test_that("dbIsValid() works", {
  # given
  h2_drv <- dbj::driver('org.h2.Driver', getOption("h2_jar"))
  con <- dbConnect(h2_drv, "jdbc:h2:mem:", 'sa')
  on.exit(dbDisconnect(con))

  # when
  valid <- dbIsValid(con)

  # then
  expect_that(valid, is_true())
})

