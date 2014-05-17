context("JDBCObject unit tests")

test_that("dbDataType(dbObj, ...) is impelemented", {
  expect_that(hasMethod("dbDataType", signature(dbObj = "JDBCObject")), is_true())
})

test_that("isSQLKeyword(dbObj, ...) is impelemented", {
  expect_that(hasMethod("isSQLKeyword", signature(dbObj = "JDBCObject")), is_true())
})

test_that("make.db.names(dbObj, ...) is impelemented", {
  expect_that(hasMethod("make.db.names", signature(dbObj = "JDBCObject")), is_true())
})

test_that("SQLKeywords(dbObj, ...) is impelemented", {
  expect_that(hasMethod("SQLKeywords", signature(dbObj = "JDBCObject")), is_true())
})

test_that("summary(object, ...) is impelemented", {
  expect_that(hasMethod("summary", signature(object = "JDBCObject")), is_true())
})

test_that("isSQLKeyword", {
  # given
  object <- structure(list(), class = "JDBCObject")
  keywords <- .SQL92Keywords

  # when
  accepted <- isSQLKeyword(object, keywords)

  expect_that(all(accepted), is_true())
})

test_that("SQLKeywords", {
  # given
  object <- structure(list(), class = "JDBCObject")
  expected_keywords <- .SQL92Keywords

  # when
  actual_keywords <- SQLKeywords(object)

  expect_that(all(expected_keywords == actual_keywords), is_true())
  expect_that(all(isSQLKeyword(object, actual_keywords)), is_true())
})