context("JDBCDriver unit tests")

test_that("has method dbUnloadDriver(drv, ...)", {
  expect_that(hasMethod("dbUnloadDriver", signature(drv = "JDBCDriver")), is_true())
})

test_that("has method dbConnect(drv, ...)", {
  expect_that(hasMethod("dbConnect", signature(drv = "JDBCDriver")), is_true())
})

test_that("has method dbGetInfo(dbObj, ...)", {
  expect_that(hasMethod("dbGetInfo", signature(dbObj = "JDBCDriver")), is_true())
})

test_that("has method dbListConnections(dbObj, ...)", {
  expect_that(hasMethod("dbListConnections", signature(drv = "JDBCDriver")), is_true())
})

test_that("has method summary(object, ...)", {
  expect_that(hasMethod("summary", signature(object = "JDBCDriver")), is_true())
})

test_that("dbIsValid returns TRUE", {
  expect_that(dbIsValid(new("JDBCDriver")), is_true())
})
