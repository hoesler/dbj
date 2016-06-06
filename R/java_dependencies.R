#' Generic to create a module definition.
#' 
#' @param x The definition.
#' @export
module <- function(x) UseMethod("module")

#' @describeIn module Construct from a string like 'group:name:version'.
#' @export
module.character <- function(x) {
  match <- regmatches(x, regexec("(.+):(.+):(.+)", x))[[1]]
  stopifnot(length(match) == 4)
  module_parts <- as.list(match[2:4])
  names(module_parts) <- c("group", "name", "version")
  module.list(module_parts)
}

#' @describeIn module Construct from a list with character elements named \code{group}, \code{name} and \code{version}.
#' @export
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
#' @export
fetch_module <- function(repository, module, ...) UseMethod("fetch_module")

#' Generic to resolve a module or file.
#' 
#' @param what The definition to resolve.
#' @param ... Additional arguments passed to methods.
#' @export
resolve <- function(what, ...) UseMethod("resolve")

#' @describeIn resolve Tries to resolve the module \code{what} in one of the given \code{repositories},
#'                installs it, and returns the local path.
#' @param repositories A list of repositories to search in.
#' @export
resolve.module <- function(what, repositories, ...) {
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
#' \code{java_classpath} additionally concatenates the resolved paths by the OS path separator.
#' 
#' @param ... Either local file paths or objects created with \code{module}.
#' @param repositories A list of repositories to search in.
#' @export
resolve_all <- function(..., repositories) {
  sapply(list(...), function(x) { resolve(x, repositories = repositories) } )
}

#' @param paths A character vector of paths, as returned by \code{resolve_all}
#' @rdname resolve_all
#' @export
as_classpath <- function(paths) {
  paste0(paths, collapse = .Platform$path.sep)
}
