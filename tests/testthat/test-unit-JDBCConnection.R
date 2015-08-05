context("JDBCConnection unit tests")

test_that("has method dbConnect(drv, ...)", {
  expect_that(hasMethod("dbConnect", signature(drv = "JDBCConnection")), is_true())
})

test_that("has method dbDisconnect(conn, ...)", {
  expect_that(hasMethod("dbDisconnect", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbSendQuery(conn, statement, ...)", {
  expect_that(hasMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character")), is_true())
})

test_that("has method dbCallProc(conn, ...)", {
  expect_that(hasMethod("dbCallProc", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbBegin(conn, ...)", {
  expect_that(hasMethod("dbBegin", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbCommit(conn, ...)", {
  expect_that(hasMethod("dbCommit", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbRollback(conn, ...)", {
  expect_that(hasMethod("dbRollback", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbListResults(conn, ...)", {
  expect_that(hasMethod("dbListResults", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbGetInfo(dbObj, ...)", {
  expect_that(hasMethod("dbGetInfo", signature(dbObj = "JDBCConnection")), is_true())
})

test_that("has method dbGetException(conn, ...)", {
  expect_that(hasMethod("dbGetException", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbListFields(conn, name, ...)", {
  expect_that(hasMethod("dbListFields", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbListTables(conn, ...)", {
  expect_that(hasMethod("dbListTables", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbReadTable(conn, name, ...)", {
  expect_that(hasMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbExistsTable(conn, name, ...)", {
  expect_that(hasMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbRemoveTable(conn, name, ...)", {
  expect_that(hasMethod("dbRemoveTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbWriteTable(conn, name, value, ...)", {
  expect_that(hasMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame")), is_true())
})

test_that("has method dbQuoteIdentifier(conn, x, ...)", {
  expect_that(hasMethod("dbQuoteIdentifier", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbQuoteString(conn, x, ...)", {
  expect_that(hasMethod("dbQuoteString", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("has method dbIsValid(dbObj, x, ...)", {
  expect_that(hasMethod("dbIsValid", signature(conn = "JDBCConnection")), is_true())
})

## extensions

test_that("has method dbGetFields(conn, ...)", {
  expect_that(hasMethod("dbGetFields", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbGetTables(conn, ...)", {
  expect_that(hasMethod("dbGetTables", signature(conn = "JDBCConnection")), is_true())
})

test_that("has method dbSendUpdate(conn, statement, parameters, ...)", {
  expect_that(hasMethod("dbSendUpdate", signature(conn = "JDBCConnection", statement = "character", parameters = "list")), is_true())
})

test_that("has method dbTruncateTable(conn, name, ...)", {
  expect_that(hasMethod("dbTruncateTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})
