# RUtils methods
context("RUtils tests")

test_that("partition.data.frame returns a list of data.frames", {
  # given
  data(iris)

  # when
  parts <- partition(iris, 100)

  # then
  expect_is(parts, "list")
  expect_true(all(sapply(parts, is.data.frame)))
})

test_that("partition.data.frame partitions correctly", {
  # given
  data(iris)

  # when
  parts <- partition(iris, 100)

  # then
  expect_equal(sapply(parts, nrow), c(100, 50))
  expect_equal(do.call(rbind, parts), iris)
})