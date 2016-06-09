#' @include java_dependencies.R
NULL

#' Resolve Java dependencies with maven
#' 
#' Define maven repositories to \link{resolve} \link[=module]{modules}.
#' 
#' @param url,path The location of the repository.
#' @param local_mirror A local maven repository to which remotely fetched modules can be installed.
#' @param install Should the fetched artifact be installed to the local repository?
#' @inheritParams fetch_module
#' @family java dependency functions
#' @name maven_repository
NULL

maven_url <- function(repository, group_id, artifact_id, version, suffix = ".jar") {
  sprintf("%s/%s/%s/%s/%s-%s%s", repository,
    gsub("\\.", "/", group_id), artifact_id, version, artifact_id, version, suffix)
}

maven_install <- function(group_id, artifact_id, version, repositories) {
  errno <- system(sprintf(
    '%s org.apache.maven.plugins:maven-dependency-plugin:2.10:get -DremoteRepositories=%s -Dartifact=%s',
    Sys.getenv("MAVEN_EXEC", "mvn"), repositories, paste0(c(group_id, artifact_id, version), collapse = ":")
  ))

  return(errno == 0)
}

#' @export
#' @rdname maven_repository
maven_remote_repository <- function(url, local_mirror = NULL, install = is(local_mirror, "maven_local_repository")) {
  structure(
    list(url = url, local_mirror = local_mirror, install = install),
    class = c("maven_remote_repository"))
}

#' @export
#' @rdname maven_repository
maven_local_repository <- function(path) {
  structure(
    list(path = path),
    class = c("maven_local_repository"))
}

#' @export
#' @rdname maven_repository
maven_local <- maven_local_repository(Sys.getenv("M2_REPO", normalizePath("~/.m2/repository")))

#' @export
#' @rdname maven_repository
maven_central <- maven_remote_repository("http://repo1.maven.org/maven2", local_mirror = maven_local)

#' @export
#' @rdname maven_repository
fetch_module.maven_remote_repository <- function(repository, module, ...) {
  if (repository$install) {
    success <- maven_install(module$group, module$name, module$version, repository$url)
    
    path <-
    if (success) {
      fetch_module(repository$local_mirror, module)
    } else {
      NULL
    }
    return(path)

  } else {
    stop("install = FALSE is not yet implemented")
  }
}

#' @export
#' @rdname maven_repository
fetch_module.maven_local_repository <- function(repository, module, ...) {
  path <- maven_url(repository$path, module$group, module$name, module$version)
  if (file.exists(path)) {
    path
  } else {
    NULL
  }
}
