context("java_utils tests")

## jtry
#######

test_that("jtry returns the evaluation result", {
  # given

  # when
  result <- jtry(.jcall("java/lang/String", "S", "valueOf", as.integer(42)))

  # then
  expect_identical(result, "42")
})

test_that("jtry calls the onError if an exception was thrown", {
  # given
  callback_counter <- 0
  error_callback <- function(...) { callback_counter <<- callback_counter + 1 }

  # when
  result <- jtry(.jthrow(.jnew("java/lang/NullPointerException")), error_callback)

  # then
  expect_identical(callback_counter, 1)
})


## verifyNotNull
################

test_that("verifyNotNull stops at .jnull", {
  expect_error(verifyNotNull(.jnull()))
})

test_that("verifyNotNull does nothing if not .jnull java object", {
  expect_silent(verifyNotNull(.jnew("java/lang/String")))
})

test_that("verifyNotNull stops at non java object", {
  expect_error(verifyNotNull("foo"))
})

## jstop
########

test_that("jstop stops at Throwable", {
  expect_error(
    jstop(.jnew("java/lang/IllegalArgumentException", "")),
    ".*IllegalArgumentException*"
  )
})

test_that("jstop stops at non java object", {
  expect_error(jstop(NULL), ".*is not a java object")
})