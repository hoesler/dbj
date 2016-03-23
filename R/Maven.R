#' Maven repository
#' 
#' Use maven repositories to locate modules.
#' 
#' @param url,path The location of the reopsitory.
#' @param install Should the fetched artifact be installed to the local repository?
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
maven_remote_repository <- function(url, install = TRUE) {
  structure(
    list(url = url, install = install),
    class = c("maven_remote_repository", "maven_repository", "module_repository"))
}

#' @export
#' @rdname maven_repository
maven_local_repository <- function(path) {
  structure(
    list(path = path),
    class = c("maven_local_repository", "maven_repository", "module_repository"))
}

#' @export
#' @rdname maven_repository
maven_central <- maven_remote_repository("http://repo1.maven.org/maven2")

#' @export
#' @rdname maven_repository
maven_local <- maven_local_repository(Sys.getenv("M2_REPO", normalizePath("~/.m2/repository")))

#' Generic to create a module definition.
#' 
#' @param x The definition.
#' @export
module <- function(x) UseMethod("module")

#' @describeIn module Construct from a string like 'group:name:version'.
module.character <- function(x) {
  match <- regmatches(x, regexec("(.+):(.+):(.+)", x))[[1]]
  stopifnot(length(match) == 4)
  module_parts <- as.list(match[2:4])
  names(module_parts) <- c("group", "name", "version")
  module.list(module_parts)
}

#' @describeIn module Construct from a list with character elements named \code{group}, \code{name} and \code{version}.
module.list <- function(x) {
  structure(
    list(
      group = as.character(get("group", x)),
      name = as.character(get("name", x)),
      version = as.character(get("version", x))
    ), class = "module")
}

#' Fetch a module from repository and make the jar locally available.
#' 
#' @param repository The repository to search in.
#' @param module The module to resolve.
#' @param ... Additional arguments passed to methods.
#' @return The path to the local version of the jar file or NULL if the module is not in the repository.
fetch_module <- function(repository, module, ...) UseMethod("fetch_module")

#' @describeIn fetch_module Get the module from the given \code{maven_remote_repository}.
fetch_module.maven_remote_repository <- function(repository, module, ...) {
  if (repository$install) {
    success <- maven_install(module$group, module$name, module$version, repository$url)
    
    path <-
    if (success) {
      fetch_module(maven_local, module)
    } else {
      NULL
    }
    return(path)

  } else {
    stop("install = FALSE is not yet implemented")
  }
}

#' @describeIn fetch_module Get the module from the given \code{maven_local_repository}.
fetch_module.maven_local_repository <- function(repository, module, ...) {
  path <- maven_url(repository$path, module$group, module$name, module$version)
  if (file.exists(path)) {
    path
  } else {
    NULL
  }
}

#' Generic to resove a module or file.
#' 
#' @param what The definition to resolve.
#' @param ... Additional arguments passed to methods.
#' @export
resolve <- function(what, ...) UseMethod("resolve")

#' @describeIn resolve Tries to resolve the module \code{what} in one of the given \code{repositories}, installs it, and returns the local path.
#' @param repositories A list of repositories to search in.
#' @export
resolve.module <- function(what, repositories = list(maven_local, maven_central), ...) {
  path <- NULL
  for (repository in repositories) {
    path <- fetch_module(repository, what)

    if (!is.null(path)) {
      message(sprintf("Found module %s in repository %s",
        paste0(what, collapse = ":"),
        paste0(repository, collapse = ",")))
      break
    }
  }
  if (is.null(path)) {
    stop(sprintf("Module %s could not be found in %s repositories",
      paste0(what, collapse = ":"), length(repositories)))
  }
  path
}

#' @describeIn resolve Checks if \code{what} is a valid path to a file and returns \code{what}.
#' @export
resolve.character <- function(what, ...) {
  stopifnot(file.exists(what))
  what
}

#' Resolve a set of module definitions.
#' 
#' \code{java_classpath} additionally concats the resolved paths by the OS path separator.
#' 
#' @param ... Either local file paths or objects created with \code{module}.
#' @param repositories A list of repositories to search in.
resolve_all <- function(..., repositories = list(maven_local, maven_central)) {
  sapply(list(...), function(x) { resolve(x, repositories = repositories) } )
}

#' @param paths A character vector of paths, as returned by \code{resolve_all}
#' @rdname resolve_all
#' @export
as_classpath <- function(paths) {
  paste0(paths, collapse = .Platform$path.sep)
}
