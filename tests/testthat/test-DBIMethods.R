# DBIConnection methods
context("DBIConnection methods")

test_that("dbConnect(drv, ...) is impelemented", {
  expect_that(existsMethod("dbConnect", signature(drv = "JDBCConnection")), is_true())
})

test_that("dbDisconnect(conn, ...) is impelemented", {
  expect_that(existsMethod("dbDisconnect", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbSendQuery(conn, statement, ...) is impelemented", {
  expect_that(existsMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character")), is_true())
})

test_that("dbGetQuery(conn, statement, ...) is impelemented", {
  expect_that(existsMethod("dbGetQuery", signature(conn = "JDBCConnection", statement = "character")), is_true())
})

test_that("dbCallProc(conn, ...) is implemented", {
  expect_that(existsMethod("dbCallProc", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbCommit(conn, ...) is implemented", {
  expect_that(existsMethod("dbCommit", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbRollback(conn, ...) is implemented", {
  expect_that(existsMethod("dbRollback", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbListResults(conn, ...) is implemented", {
  expect_that(existsMethod("dbListResults", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbGetInfo(dbObj, ...) is impelemented", {
  expect_that(existsMethod("dbGetInfo", signature(dbObj = "JDBCConnection")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(existsMethod("summary", signature(object = "JDBCConnection")), is_true())
})

test_that("dbGetException(conn, ...) is implemented", {
  expect_that(existsMethod("dbGetException", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbListFields(conn, name, ...) is implemented", {
  expect_that(existsMethod("dbListFields", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("dbListTables(conn, ...) is implemented", {
  expect_that(existsMethod("dbListTables", signature(conn = "JDBCConnection")), is_true())
})

test_that("dbReadTable(conn, name, ...) is implemented", {
  expect_that(existsMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("dbExistsTable(conn, name, ...) is implemented", {
  expect_that(existsMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("dbRemoveTable(conn, name, ...) is implemented", {
  expect_that(existsMethod("dbRemoveTable", signature(conn = "JDBCConnection", name = "character")), is_true())
})

test_that("dbWriteTable(conn, name, value, ...) is implemented", {
  expect_that(existsMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame")), is_true())
})

# DBIDriver methods
context("DBIDriver methods")

test_that("dbUnloadDriver(drv, ...) is impelemented", {
  expect_that(existsMethod("dbUnloadDriver", signature(drv = "JDBCDriver")), is_true())
})

test_that("dbConnect(drv, ...) is impelemented", {
  expect_that(existsMethod("dbConnect", signature(drv = "JDBCDriver")), is_true())
})

test_that("dbGetInfo(dbObj, ...) is impelemented", {
  expect_that(existsMethod("dbGetInfo", signature(dbObj = "JDBCDriver")), is_true())
})

test_that("dbListConnections(dbObj, ...) is impelemented", {
  expect_that(existsMethod("dbListConnections", signature(drv = "JDBCDriver")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(existsMethod("summary", signature(object = "JDBCDriver")), is_true())
})

# DBIObject methods
context("DBIObject methods")

test_that("dbDataType(dbObj, ...) is impelemented", {
  expect_that(existsMethod("dbDataType", signature(dbObj = "JDBCObject")), is_true())
})

test_that("isSQLKeyword(dbObj, ...) is impelemented", {
  expect_that(existsMethod("isSQLKeyword", signature(dbObj = "JDBCObject")), is_true())
})

test_that("make.db.names(dbObj, ...) is impelemented", {
  expect_that(existsMethod("make.db.names", signature(dbObj = "JDBCObject")), is_true())
})

test_that("SQLKeywords(dbObj, ...) is impelemented", {
  expect_that(existsMethod("SQLKeywords", signature(dbObj = "JDBCObject")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(existsMethod("summary", signature(object = "JDBCObject")), is_true())
})

# DBIResult methods
context("DBIResult methods")

test_that("fetch(res, n, ...) is impelemented", {
  expect_that(existsMethod("fetch", signature(res = "JDBCResult", n = "numeric")), is_true())
})

test_that("fetch(res, n, ...) is impelemented", {
  expect_that(existsMethod("fetch", signature(res = "JDBCResult", n = "missing")), is_true())
})

test_that("dbClearResult(res, ...) is impelemented", {
  expect_that(existsMethod("dbClearResult", signature(res = "JDBCResult")), is_true())
})

test_that("dbColumnInfo(res, ...) is impelemented", {
  expect_that(existsMethod("dbColumnInfo", signature(res = "JDBCResult")), is_true())
})

test_that("dbGetException(conn, ...) is impelemented", {
  expect_that(existsMethod("dbGetException", signature(conn = "JDBCResult")), is_true())
})

test_that("dbGetInfo(dbObj, ...) is impelemented", {
  expect_that(existsMethod("dbGetInfo", signature(dbObj = "JDBCResult")), is_true())
})

test_that("dbGetRowCount(res, ...) is impelemented", {
  expect_that(existsMethod("dbGetRowCount", signature(res = "JDBCResult")), is_true())
})

test_that("dbGetRowsAffected(res, ...) is impelemented", {
  expect_that(existsMethod("dbGetRowsAffected", signature(res = "JDBCResult")), is_true())
})

test_that("dbGetStatement(res, ...) is impelemented", {
  expect_that(existsMethod("dbGetStatement", signature(res = "JDBCResult")), is_true())
})

test_that("dbHasCompleted(res, ...) is impelemented", {
  expect_that(existsMethod("dbHasCompleted", signature(res = "JDBCResult")), is_true())
})

test_that("dbListFields(conn, name, ...) is impelemented", {
  expect_that(existsMethod("dbListFields", signature(conn = "JDBCResult", name = "missing")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(existsMethod("summary", signature(object = "JDBCResult")), is_true())
})

test_that("coerce(from, to, ...) is impelemented", {
  expect_that(existsMethod("coerce", signature(from = "JDBCConnection", to = "JDBCResult")), is_true())
})
