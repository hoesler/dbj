context("JDBCObject unit tests")

test_that("dbDataType(dbObj, ...) is impelemented", {
  expect_that(hasMethod("dbDataType", signature(dbObj = "JDBCObject")), is_true())
})

test_that("SQLKeywords(dbObj, ...) is impelemented", {
  expect_that(hasMethod("SQLKeywords", signature(dbObj = "JDBCObject")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(hasMethod("summary", signature(object = "JDBCObject")), is_true())
})

test_that("dbIsValid(dbObj, ...) is impelemented", {
  expect_that(hasMethod("dbIsValid", signature(object = "JDBCObject")), is_true())
})
