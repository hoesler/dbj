context("JDBCResult unit tests")

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCResult", n = "numeric")), is_true())
})

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCResult", n = "missing")), is_true())
})

test_that("has method dbClearResult(res, ...)", {
  expect_that(hasMethod("dbClearResult", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbColumnInfo(res, ...)", {
  expect_that(hasMethod("dbColumnInfo", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbGetException(conn, ...)", {
  expect_that(hasMethod("dbGetException", signature(conn = "JDBCResult")), is_true())
})

test_that("has method dbGetInfo(dbObj, ...)", {
  expect_that(hasMethod("dbGetInfo", signature(dbObj = "JDBCResult")), is_true())
})

test_that("has method dbGetRowCount(res, ...)", {
  expect_that(hasMethod("dbGetRowCount", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbGetRowsAffected(res, ...)", {
  expect_that(hasMethod("dbGetRowsAffected", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbGetStatement(res, ...)", {
  expect_that(hasMethod("dbGetStatement", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbHasCompleted(res, ...)", {
  expect_that(hasMethod("dbHasCompleted", signature(res = "JDBCResult")), is_true())
})

test_that("has method dbListFields(conn, name, ...)", {
  expect_that(hasMethod("dbListFields", signature(conn = "JDBCResult", name = "missing")), is_true())
})

test_that("has method summary(object, ...)", {
  expect_that(hasMethod("summary", signature(object = "JDBCResult")), is_true())
})

test_that("has method coerce(from, to, ...)", {
  expect_that(hasMethod("coerce", signature(from = "JDBCConnection", to = "JDBCResult")), is_true())
})
