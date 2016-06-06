context("java_dependencies")

test_that("resolve.character", {
  file1 <- tempfile()
  file.create(file1)
  file2 <- tempfile()
  file.create(file2)

  expect_equal(resolve(c(file1, file2)), c(file1, file2))

  expect_error(resolve(tempfile()))
})

test_that("resolve.list", {
  file1 <- tempfile()
  file.create(file1)
  file2 <- tempfile()
  file.create(file2)

  expect_equal(
    resolve(list(
      file1,
      file2
    )),
    c(file1, file2)
  )
})