# JavaUtils methods
context("JavaUtils unit tests")

test_that("jtry returns the evaluation result", {
  # given

  # when
  result <- jtry(.jcall("java/lang/String", "S", "valueOf", as.integer(42)))

  # then
  expect_that(result, equals("42"))
})

test_that("jtry calls the onError if an exception was thrown", {
  # given
  callback_counter <- 0
  error_callback <- function(e) { callback_counter <<- callback_counter + 1 }

  # when
  result <- jtry(.jthrow(.jnew("java/lang/NullPointerException")), error_callback)

  # then
  expect_that(callback_counter, equals(1))
})

test_that("verifyNotNull stops at .jnull", {
  expect_that(verifyNotNull(.jnull()), throws_error())
})

test_that("verifyNotNull does nothing if not .jnull java object", {
  expect_that(verifyNotNull(.jnew("java/lang/String")), not(throws_error()))
})

test_that("verifyNotNull stops at non java object", {
  expect_that(verifyNotNull("foo"), throws_error())
})