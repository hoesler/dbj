maven_url <- function(group_id, artifact_id, version, suffix = ".jar") {
	sprintf("http://search.maven.org/remotecontent?filepath=%s/%s/%s/%s-%s%s",
		gsub("\\.", "/", group_id), artifact_id, version, artifact_id, version, suffix)
}

wget <- function(source, dest) {
	message(paste("Downloading", source))
	  
	# appveyor cannot verify repo1.maven.org's certificate
	if (.Platform$OS.type == "windows") {
		options("download.file.extra" = "--no-check-certificate")
	}
	
	failed <- download.file(source, dest, "wget", quiet = TRUE) # internal fails because of a 302 response
	if (failed) {
	  stop("File could not be downloaded")
	} else {
	  if (!file.exists(dest)) {
	    stop(sprintf("File was downloaded but %s is missing", dest))
	  }
	}
}

h2_jar <- function(h2_version = "1.3.176") {
	if (!is.character(getOption("h2_jar")) || !file.exists(getOption("h2_jar"))) {
		h2_jar_url <- maven_url('com.h2database', 'h2', h2_version)
		h2_jar_dest <- file.path(tempdir(), sprintf("h2-%s.jar", h2_version))
		wget(h2_jar_url, h2_jar_dest)
		options(h2_jar = h2_jar_dest)
	}

	if (!file.exists(getOption("h2_jar"))) stop("Path to h2 jar could not be resolved")
	
	getOption("h2_jar")
}

h2_driver_class <- function() 'org.h2.Driver'