library("testthat")

# Ensure database creation done before tests
h2_version <- "1.3.176"
h2_jar_src <- sprintf("http://search.maven.org/remotecontent?filepath=com/h2database/h2/%s/h2-%s.jar", h2_version, h2_version)
h2_jar_dest <- sprintf("h2-%s.jar", h2_version)
if (!file.exists(h2_jar_dest)) {
  print("Downloading H2 jar")
  failed <- download.file(h2_jar_src, h2_jar_dest, "wget") # internal fails because of a 302 response
  if (failed) {
    stop("H2 Database Jar could not be downloaded")
  } else {
    if (!file.exists(h2_jar_dest)) {
      stop("H2 file was downloaded but is missing")
    }
  }
}
options(h2_jar = file.path(getwd(), h2_jar_dest))
if (!is.character(getOption("h2_jar"))) stop("Path to h2 jar could not be resolved")

expect_contained <- function(a, b) {
	expect_that(a, is_contained_in(b))
}

is_contained_in <- function(expected_elements, label = NULL, ...) {
	function(actual_elements) {
		contained <- actual_elements %in% expected_elements
		expectation(
			all(contained),
			paste0("[", paste0(expected_elements, collapse = ", "), "] does not contain [", paste0(actual_elements[!contained], collapse = ", "), "]"),
			paste0("expected_elements contains all ", length(actual_elements), " elements in actual_elements")
		)
	}
}

test_check("dbj", filter = "unit-.*")
test_check("dbj", filter = "integration-.*")