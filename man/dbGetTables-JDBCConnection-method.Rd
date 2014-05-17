\docType{methods}
\name{dbGetTables,JDBCConnection-method}
\alias{dbGetTables,JDBCConnection-method}
\title{Get a description of the tables available in the given catalog.}
\usage{
\S4method{dbGetTables}{JDBCConnection}(conn, pattern = "\%", ...)
}
\arguments{
  \item{conn}{a \code{\linkS4class{JDBCConnection}} object,
  produced by \code{\link[DBI]{dbConnect}}}

  \item{pattern}{the pattern for table names}

  \item{...}{Ignored. Needed for compatibility with
  generic.}
}
\description{
Get a description of the tables available in the given
catalog.
}

