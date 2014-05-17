\docType{methods}
\name{dbListFields,JDBCConnection,character-method}
\alias{dbListFields,JDBCConnection,character-method}
\title{List field names in specified table.}
\usage{
\S4method{dbListFields}{JDBCConnection,character}(conn, name, pattern = "\%",
  ...)
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{name}{character vector of length 1 giving name of
  the table}

  \item{pattern}{the pattern for the columns to list}

  \item{...}{Ignored. Included for compatibility with
  generic}
}
\description{
List field names in specified table.
}

