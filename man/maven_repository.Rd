% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/java_dependencies_maven.R
\docType{data}
\name{maven_repository}
\alias{fetch_module.maven_local_repository}
\alias{fetch_module.maven_remote_repository}
\alias{maven_central}
\alias{maven_local}
\alias{maven_local_repository}
\alias{maven_remote_repository}
\alias{maven_repository}
\title{Resolve Java dependencies with maven}
\format{An object of class \code{maven_local_repository} of length 1.}
\usage{
maven_remote_repository(url, local_mirror = NULL, install = is(local_mirror,
  "maven_local_repository"))

maven_local_repository(path)

maven_local

maven_central

\method{fetch_module}{maven_remote_repository}(repository, module, ...)

\method{fetch_module}{maven_local_repository}(repository, module, ...)
}
\arguments{
\item{url, path}{The location of the repository.}

\item{local_mirror}{A local maven repository to which remotely fetched modules can be installed.}

\item{install}{Should the fetched artifact be installed to the local repository?}

\item{repository}{The repository to search in.}

\item{module}{The module to resolve.}

\item{...}{Additional arguments passed to methods.}
}
\description{
Define maven repositories to \link{resolve} \link[=module]{modules}.
}
\seealso{
Other java dependency functions: \code{\link{fetch_module}},
  \code{\link{module}}, \code{\link{resolve}}
}
\keyword{datasets}

