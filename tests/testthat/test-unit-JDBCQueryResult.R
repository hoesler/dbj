context("JDBCQueryResult unit tests")

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCQueryResult", n = "numeric")), is_true())
})

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCQueryResult", n = "missing")), is_true())
})

test_that("has method dbClearResult(res, ...)", {
  expect_that(hasMethod("dbClearResult", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbColumnInfo(res, ...)", {
  expect_that(hasMethod("dbColumnInfo", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbGetException(conn, ...)", {
  expect_that(hasMethod("dbGetException", signature(conn = "JDBCQueryResult")), is_true())
})

test_that("has method dbGetInfo(dbObj, ...)", {
  expect_that(hasMethod("dbGetInfo", signature(dbObj = "JDBCQueryResult")), is_true())
})

test_that("has method dbGetRowCount(res, ...)", {
  expect_that(hasMethod("dbGetRowCount", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbGetRowsAffected(res, ...)", {
  expect_that(hasMethod("dbGetRowsAffected", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbGetStatement(res, ...)", {
  expect_that(hasMethod("dbGetStatement", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbHasCompleted(res, ...)", {
  expect_that(hasMethod("dbHasCompleted", signature(res = "JDBCQueryResult")), is_true())
})

test_that("has method dbListFields(conn, name, ...)", {
  expect_that(hasMethod("dbListFields", signature(conn = "JDBCQueryResult", name = "missing")), is_true())
})

test_that("has method summary(object, ...)", {
  expect_that(hasMethod("summary", signature(object = "JDBCQueryResult")), is_true())
})

test_that("has method coerce(from, to, ...)", {
  expect_that(hasMethod("coerce", signature(from = "JDBCConnection", to = "JDBCQueryResult")), is_true())
})
