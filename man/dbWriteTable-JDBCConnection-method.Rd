\docType{methods}
\name{dbWriteTable,JDBCConnection-method}
\alias{dbWriteTable,JDBCConnection-method}
\title{Write a local data frame or file to the database.}
\usage{
\S4method{dbWriteTable}{JDBCConnection}(conn, name, value, overwrite = TRUE,
  append = FALSE, ...)
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{name}{character vector of length 1 giving name of
  table to write to}

  \item{value}{the date frame to write to the table}

  \item{overwrite}{a logical value indicating if the table
  should be overwritten if it exists}

  \item{append}{a logical value indicating if the data
  shuld get appended to an existing table}

  \item{...}{ any optional arguments that the underlying
  database driver supports, e.g.  \describe{
  \item{row.names}{ in the case of \code{dbReadTable}, this
  argument can be a string or an index specifying the
  column in the DBMS table to be used as \code{row.names}
  in the output data.frame (a \code{NULL}, \code{""}, or 0
  specifies that no column should be used as
  \code{row.names} in the output). In the case of
  \code{dbWriteTable}, this argument should be a logical
  specifying whether the \code{row.names} should be output
  to the output DBMS table; if \code{TRUE}, the extra field
  name will be whatever the S identifier \code{"row.names"}
  maps to the DBMS (see \code{\link{make.db.names}}).  }
  \item{overwrite}{ a logical specifying whether to
  overwrite an existing table or not.  Its default is
  \code{FALSE}.  } \item{append}{ a logical specifying
  whether to append to an existing table in the DBMS.  Its
  default is \code{FALSE}.  } } }
}
\description{
Write a local data frame or file to the database.
}

