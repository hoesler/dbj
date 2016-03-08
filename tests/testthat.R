library("testthat")

maven_jar <- function(group_id, artifact_id, version) {
	sprintf("http://search.maven.org/remotecontent?filepath=%s/%s/%s/%s-%s.jar",
		gsub("\\.", "/", group_id), artifact_id, version, artifact_id, version)
}

rsync_file <- function(source, dest) {
	if (!file.exists(dest)) {
	  message(paste("Downloading", source))
	  
	  # appveyor cannot verify repo1.maven.org's certificate
	  if (.Platform$OS.type == "windows") {
	  	options("download.file.extra" = "--no-check-certificate")
	  }
	  
	  failed <- download.file(source, dest, "wget") # internal fails because of a 302 response
	  if (failed) {
	    stop("File could not be downloaded")
	  } else {
	    if (!file.exists(dest)) {
	      stop(sprintf("File was downloaded but %s is missing", dest))
	    }
	  }
	}
}

# Ensure database creation done before tests
h2_version <- "1.3.176"
h2_jar_src <- maven_jar('com.h2database', 'h2', h2_version)
h2_jar_dest <- sprintf("h2-%s.jar", h2_version)
rsync_file(h2_jar_src, h2_jar_dest)
options(h2_jar = file.path(getwd(), h2_jar_dest))
if (!is.character(getOption("h2_jar"))) stop("Path to h2 jar could not be resolved")

source("expectations-contained.R")

test_check("dbj", filter = "unit-.*")
test_check("dbj", filter = "integration-.*")
test_check("dbj", filter = "DBItest")