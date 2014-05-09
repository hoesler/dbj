\docType{methods}
\name{dbGetFields,JDBCConnection-method}
\alias{dbGetFields,JDBCConnection-method}
\title{Get description of table columns available in the specified catalog.}
\usage{
\S4method{dbGetFields}{JDBCConnection}(conn, ..., name, pattern = "\%")
}
\arguments{
  \item{conn}{a \code{\linkS4class{JDBCConnection}} object,
  produced by \code{\link[DBI]{dbConnect}}}

  \item{...}{Ignored. Needed for compatibility with
  generic.}

  \item{name}{the pattern for table names}

  \item{pattern}{the pattern for column names}
}
\description{
Get description of table columns available in the specified
catalog.
}

