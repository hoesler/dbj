# RUtils methods
context("RUtils tests")

test_that("partition.data.frame returns a list of data.frames", {
  # given
  data(iris)

  # when
  parts <- partition(iris, 100)

  # then
  expect_that(parts, is_a("list"))
  expect_that(all(sapply(parts, is.data.frame)), is_true())
})

test_that("partition.data.frame partitions correctly", {
  # given
  data(iris)

  # when
  parts <- partition(iris, 100)

  # then
  expect_that(sapply(parts, nrow), equals(c(100, 50)))
  expect_that(do.call(rbind, parts), equals(iris))
})