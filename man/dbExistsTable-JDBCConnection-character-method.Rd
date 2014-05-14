\docType{methods}
\name{dbExistsTable,JDBCConnection,character-method}
\alias{dbExistsTable,JDBCConnection,character-method}
\title{Does the table exist?}
\usage{
\S4method{dbExistsTable}{JDBCConnection,character}(conn, name, ...)
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{name}{character vector of length 1 giving name of
  table}

  \item{...}{Ignored. Included for compatibility with
  generic.}
}
\description{
Does the table exist?
}

