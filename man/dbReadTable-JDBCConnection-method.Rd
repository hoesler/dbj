\docType{methods}
\name{dbReadTable,JDBCConnection-method}
\alias{dbReadTable,JDBCConnection-method}
\title{Convenience functions for importing/exporting DBMS tables}
\usage{
\S4method{dbReadTable}{JDBCConnection}(conn, name, ...)
}
\arguments{
  \item{conn}{a \code{\linkS4class{JDBCConnection}} object,
  produced by \code{\link[DBI]{dbConnect}}}

  \item{name}{a character string specifying a table name.}

  \item{...}{Ignored. Needed for compatibility with
  generic.}
}
\description{
Convenience functions for importing/exporting DBMS tables
}

