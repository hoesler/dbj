#' Define Java dependencies as a module
#'
#' The \code{module} function creates a definition object of a Java dependency,
#' which can be \code{\link[=resolve]{resolved}} from a \code{\link{repository}}
#' and used to create a classpath argument to \code{\link{driver}}.
#'
#' @param x A module definition
#' @param transitive A logical indicating if transitive dependencies should be fetched as well
#' @param ... Additional arguments passed to methods.
#' @return a list with class \code{"module"}
#' @family java dependency functions
#' @export
#' @examples
#' module('com.h2database:h2:1.3.176')
#' module('com.h2database:h2:1.3.176', transitive = FALSE)
#' module(list(group = 'com.h2database', name = 'h2', version = '1.3.176'))
module <- function(x, ...) UseMethod("module")

#' @describeIn module Constructs a module from a string in the form 'group:name:version'.
#' @export
module.character <- function(x, transitive = TRUE, ...) {
  match <- regmatches(x, regexec("(.+):(.+):(.+)", x))[[1]]
  stopifnot(length(match) == 4)
  module_parts <- as.list(match[2:4])
  names(module_parts) <- c("group", "name", "version")
  module.list(module_parts, transitive = transitive)
}

#' @describeIn module Constructs a module from a named list with elements \code{group}, \code{name} and \code{version}.
#' @export
module.list <- function(x, transitive = TRUE, ...) {
  x <- as.environment(x)
  structure(
    list(
      group = as.character(get("group", x)),
      name = as.character(get("name", x)),
      version = as.character(get("version", x)),
      ext = as.character(get0("ext", x, ifnotfound = "jar")),
      transitive = transitive
    ), class = "module")
}

#' Fetch a module from a repository
#'
#' Classes which support \code{fetch_module} should locate the given \code{module} and make it locally available.
#'
#' @param repository The repository to search in.
#' @param module The module to resolve.
#' @param ... Additional arguments passed to methods.
#' @return The path to the local version of the jar file or NULL if the module is not in the repository.
#' @aliases repository
#' @family java dependency functions
#' @export
fetch_module <- function(repository, module, ...) UseMethod("fetch_module")

#' Resolve objects to file paths
#'
#' @param what The definition to resolve.
#' @param ... Additional arguments passed to methods.
#' @family java dependency functions
#' @export
#' @examples
#' resolve(module('com.h2database:h2:1.3.176'), list(maven_local, maven_central))
resolve <- function(what, ...) UseMethod("resolve")

#' @describeIn resolve Tries to resolve the module \code{what} in one of the given \code{repositories}
#'  using \code{\link{fetch_module}}.
#' @param repositories A list of \code{\link[=repository]{repositories}} to search in.
#' @inheritParams fetch_module
#' @export
resolve.module <- function(what, repositories, ...) {
  path <- NULL
  for (repository in repositories) {
    path <- fetch_module(repository, what, ...)

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

#' @describeIn resolve Resolves all elements in \code{what} and returns them as a list.
#' @export
resolve.list <- function(what, ...) {
  sapply(what, resolve, ...)
}
