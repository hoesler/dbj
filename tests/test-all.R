library("testthat")

# Ensure database creation done before tests
h2_version <- "1.3.176"
h2_jar_src <- sprintf("http://search.maven.org/remotecontent?filepath = com/h2database/h2/%s/h2-%s.jar", h2_version, h2_version)
h2_jar_dest <- "h2.jar"
if (!file.exists(h2_jar_dest)) {
  print("Downloading H2 jar")
  failed <- download.file(h2_jar_src, h2_jar_dest, "auto")
  if (failed) {
    stop("H2 Database Jar could not be downloaded")
  } else {
    if (!file.exists(h2_jar_dest)) {
      stop("H2 file was downloaded but is missing")
    }
  }
}
print(sprintf("H2 jar location: %s", file.path(getwd(), h2_jar_dest)))

test_check("RJDBC", filter = "unit-.*")
test_check("RJDBC", filter = "integration-.*")