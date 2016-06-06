context("java_dependencies_maven")

test_that("resolve.module resolves a maven module", {
  module <- module('com.h2database:h2:1.3.176')
  path <- resolve(module, list(maven_local))
  expect_true(file.exists(path))
})