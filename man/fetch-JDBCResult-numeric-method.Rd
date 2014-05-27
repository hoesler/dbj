\docType{methods}
\name{fetch,JDBCResult,missing-method}
\alias{fetch,JDBCResult,missing-method}
\alias{fetch,JDBCResult,numeric-method}
\title{Fetch records from a previously executed query}
\usage{
\S4method{fetch}{JDBCResult,missing}(res, n, ...)

\S4method{fetch}{JDBCResult,numeric}(res, n, ...)
}
\arguments{
  \item{res}{an \code{\linkS4class{JDBCResult}} object.}

  \item{n}{optional maximum number of records to retrieve
  per fetch. Use \code{-1} to retrieve all pending records;
  use \code{0} for to fetch the default number of rows as
  defined in \code{\link{JDBC}}}

  \item{...}{Ignored. Needed for compatibility with
  generic.}
}
\description{
Fetch records from a previously executed query
}

