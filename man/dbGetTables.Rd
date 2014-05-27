\docType{methods}
\name{dbGetTables,JDBCConnection-method}
\alias{dbGetTables}
\alias{dbGetTables,JDBCConnection-method}
\title{Generics for getting a description of the tables available in the given catalog.}
\usage{
\S4method{dbGetTables}{JDBCConnection}(conn, pattern = "\%", ...)

dbGetTables(conn, ...)
}
\arguments{
  \item{pattern}{the pattern for table names}

  \item{conn}{An \code{DBIConnection} object.}

  \item{...}{Other arguments used by methods}
}
\description{
Generics for getting a description of the tables available
in the given catalog.
}

