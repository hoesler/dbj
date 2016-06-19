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

maven_install <- function(group_id, artifact_id, version, repositories, transitive = TRUE) {
  error_code <- system2(
    command = Sys.getenv("MAVEN_EXEC", "mvn"),
    args = c(
      'org.apache.maven.plugins:maven-dependency-plugin:2.10:get',
      paste0('-DremoteRepositories=', paste(repositories, sep = ",")),
      paste0('-Dartifact=', paste0(c(group_id, artifact_id, version), collapse = ":")),
      paste0('-Dtransitive=', as.character(transitive))
    ),
    stdout = FALSE
  )

  stopifnot(error_code == 0)
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
    success <- maven_install(module$group, module$name, module$version, repository$url, transitive = module$transitive)

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

maven_dependencies <- function(pom) {
  outfile <- tempfile()
  on.exit({ if (file.exists(outfile)) file.remove(outfile) })

  error_code <- system2(
    command = Sys.getenv("MAVEN_EXEC", "mvn"),
    args = c(
      paste('-f', pom),
      'dependency:list',
      '-Dmdep.outputScope=FALSE',
      '-DincludeScope=runtime',
      paste0('-DoutputFile=', outfile)
    ),
    stdout = FALSE
  )

  stopifnot(error_code == 0)

  output <- readLines(outfile)
  matches <- regmatches(output, regexec("\\s+(.+):(.+):(.+):(.+)", output))

  unique(unlist(Map(function(x) paste0(x[2], ":", x[3], ":", x[5]), Filter(length, matches))))
}

#' @export
#' @rdname maven_repository
fetch_module.maven_local_repository <- function(repository, module, ...) {

  modules <- list(module)
  if (module$transitive) {
    pom <- maven_url(repository$path, module$group, module$name, module$version, '.pom')
    deps <- maven_dependencies(pom)
    modules <- c(modules, unname(lapply(deps, dbj::module)))
  }

  sapply(modules, function(x) {
    path <- maven_url(repository$path, x$group, x$name, x$version)
    if (file.exists(path)) {
      path
    } else {
      NULL
    }
  })
}
