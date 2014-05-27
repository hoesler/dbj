\docType{methods}
\name{dbGetFields,JDBCConnection-method}
\alias{dbGetFields}
\alias{dbGetFields,JDBCConnection-method}
\title{Generics for getting a description of table columns available in the specified catalog.}
\usage{
\S4method{dbGetFields}{JDBCConnection}(conn, name, pattern = "\%", ...)

dbGetFields(conn, ...)
}
\arguments{
  \item{name}{the pattern for table names}

  \item{pattern}{the pattern for column names}

  \item{conn}{An \code{DBIConnection} object.}

  \item{...}{Other arguments used by methods}
}
\description{
Generics for getting a description of table columns
available in the specified catalog.
}

