\docType{methods}
\name{dbListFields,JDBCConnection,character-method}
\alias{dbListFields,JDBCConnection,character-method}
\title{Get a description of table columns available in the specified catalog.}
\usage{
\S4method{dbListFields}{JDBCConnection,character}(conn, name, ...,
  pattern = "\%")
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{name}{character vector of length 1 giving name of
  the table}

  \item{...}{Ignored. Included for compatibility with
  generic.}

  \item{pattern}{the pattern for the columns to list}
}
\description{
Get a description of table columns available in the
specified catalog.
}

