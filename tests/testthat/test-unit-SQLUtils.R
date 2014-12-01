# SQLUtils methods
context("SQLUtils unit tests")

test_that("sql_escape escapes each element in character vector", {
  # given
  identifiers <- c("fo_o", "ba$r")

  # when
  escpaed <- sql_escape(identifiers, "'", TRUE)

  # then
  expect_that(escpaed, equals(c("'fo_o'", "'ba$r'")))
})