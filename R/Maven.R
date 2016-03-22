maven_url <- function(repository, group_id, artifact_id, version, suffix = ".jar") {
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

#' Create a maven repository definition
#' @param url The url of the reopsitory.
#' @export
maven_repository <- function(url) {
  structure(list(url = url), class = c("maven_repository", "module_repository"))
}

#' @export
#' @rdname maven_repository
maven_central <- maven_repository("http://repo1.maven.org/maven2")

#' Generic to create a module definition.
#' @param x The definition.
#' @export
module <- function(x) UseMethod("module")

#' @describeIn module Construct from a string like 'group:name:version'.
module.character <- function(x) {
  match <- regmatches(x, regexec("(.+):(.+):(.+)", x))[[1]]
  stopifnot(length(match) == 4)
  module_parts <- as.list(match[2:4])
  names(module_parts) <- c("group", "name", "version")
  module(module_parts)
}

#' @describeIn module Construct from a list with character elements named \code{group}, \code{name} and \code{version}.
module.list <- function(x) {
  structure(
    list(
      group = as.character(get("group", x)),
      name = as.character(get("name", x)),
      version = as.character(get("version", x))
    ), class = "module_definition")
}

#' Resolve a module in a given repository.
#' @param repository The repository to search in.
#' @param module The module to resolve.
#' @param ... Additional arguments passed to methods.
resolve <- function(repository, module, ...) UseMethod("resolve")

#' @describeIn resolve Resove in a \code{maven_repository}.
resolve.maven_repository <- function(repository, module, ...) {
  local_jar <- maven_url(
    Sys.getenv("M2_REPO", normalizePath("~/.m2/repository")),
    module$group, module$name, module$version)

  if (!file.exists(local_jar)) {
    maven_install(module$group, module$name, module$version, repository$url)
  }

  local_jar
}

#' Generic to resove the path a path.
#' @param what The definition to resolve.
#' @param ... Additional arguments passed to methods.
#' @export
as.path <- function(what, ...) UseMethod("as.path")

#' @describeIn as.path Try to resolve a module in one of the given repositories, install it, and return the local path.
#' @param repositories A list of repositories to search in.
#' @export
as.path.module_definition <- function(what, repositories, ...) {
  path <- NULL
  for (repository in repositories) {
    if (!is.null(path)) {
      break
    } else {
      path <- resolve(repository, what)
    }
  }
  path
}

#' @describeIn as.path Simple returns the input
#' @export
as.path.character <- function(what, ...) what

#' Create a classpath from the arguments.
#' Character verctors are treated as local file paths.
#' \code{module} objects will be resolved.
#' 
#' @param ... Either local file paths or objects created with \code{module}.
#' @param repositories The repositories used to resolve modules.
#' @export
java_classpath <- function(..., repositories = list(maven_central)) {
  paths <- sapply(list(...), function(x) { as.path(x, repositories) } )
  paste0(paths, collapse = ":")
}
