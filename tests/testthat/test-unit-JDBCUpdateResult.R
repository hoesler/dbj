context("JDBCUpdateResult unit tests")

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCUpdateResult", n = "numeric")), is_true())
})

test_that("has method fetch(res, n, ...)", {
  expect_that(hasMethod("fetch", signature(res = "JDBCUpdateResult", n = "missing")), is_true())
})

test_that("has method dbClearResult(res, ...)", {
  expect_that(hasMethod("dbClearResult", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("has method dbColumnInfo(res, ...)", {
  expect_that(hasMethod("dbColumnInfo", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("has method dbGetInfo(dbObj, ...)", {
  expect_that(hasMethod("dbGetInfo", signature(dbObj = "JDBCUpdateResult")), is_true())
})

test_that("dbGetRowCount throw error", {
  expect_error(dbGetRowCount(new("JDBCUpdateResult")))
})

test_that("has method dbGetRowsAffected(res, ...)", {
  expect_that(hasMethod("dbGetRowsAffected", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("has method dbGetStatement(res, ...)", {
  expect_that(hasMethod("dbGetStatement", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("dbHasCompleted returns TRUE", {
  expect_that(dbHasCompleted(new("JDBCUpdateResult")), is_true())
})

test_that("dbIsValid returns TRUE", {
  expect_that(dbIsValid(new("JDBCUpdateResult")), is_true())
})
