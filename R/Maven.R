#' Maven utils
#' @param group_id The group id of the artifact
#' @param artifact_id The id of the artifact
#' @param version The version of the artifact
#' @param local_repository The path to the local repository
#' @param remote_repository The url of the remote repository
#' @name maven
NULL

#' @export
#' @rdname maven
maven_central <- "http://repo1.maven.org/maven2"

#' @export
#' @rdname maven
default_maven_local <- Sys.getenv("M2_REPO", normalizePath("~/.m2/repository"))

maven_url <- function(group_id, artifact_id, version, suffix = ".jar", repository) {
  sprintf("%s/%s/%s/%s/%s-%s%s", repository,
    gsub("\\.", "/", group_id), artifact_id, version, artifact_id, version, suffix)
}

maven_install <- function(group_id, artifact_id, version, repositories) {
  errno <- system(sprintf(
    '%s org.apache.maven.plugins:maven-dependency-plugin:2.10:get -DremoteRepositories=%s -Dartifact=%s',
    Sys.getenv("MAVEN_EXEC", "mvn"), repositories, paste0(c(group_id, artifact_id, version), collapse = ":")
  ))

  if (errno != 0) {
    stop(sprintf("Installing artifact failed"))
  }
}

#' Get the path to the desired artifact in the \code{local_repository} and install from the \code{remote_repository} if missing.
#' @return The path to the jar file
#' @export
#' @rdname maven
maven_jar <- function(group_id, artifact_id, version,
  local_repository = default_maven_local, remote_repository = maven_central) {

  local_jar <- maven_url(group_id, artifact_id, version, local_repository)
  
  if (!file.exists(local_jar)) {
    maven_install(group_id, artifact_id, version, remote_repository)

    if (!file.exists(local_jar)) {
      stop(sprintf("File was installed but %s is missing", local_jar))
    }
  }

  local_jar
}
