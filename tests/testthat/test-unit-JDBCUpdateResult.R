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

test_that("has method dbGetRowCount(res, ...)", {
  expect_that(hasMethod("dbGetRowCount", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("dbGetRowsAffected throw error", {
  expect_error(dbGetRowsAffected(new(JDBCUpdateResult)))
})

test_that("has method dbGetStatement(res, ...)", {
  expect_that(hasMethod("dbGetStatement", signature(res = "JDBCUpdateResult")), is_true())
})

test_that("dbHasCompleted returns TRUE", {
  expect_that(dbHasCompleted, new(JDBCUpdateResult)), is_true())
})

test_that("has method dbListFields(conn, name, ...)", {
  expect_that(hasMethod("dbListFields", signature(conn = "JDBCUpdateResult", name = "missing")), is_true())
})

test_that("dbIsValid returns TRUE", {
  expect_that(dbIsValid(new("JDBCUpdateResult")), is_true())
})
