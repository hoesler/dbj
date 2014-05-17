\docType{methods}
\name{dbSendUpdate,JDBCConnection,character-method}
\alias{dbSendUpdate,JDBCConnection,character-method}
\title{Execute a SQL statement on a database connection to update the database}
\usage{
\S4method{dbSendUpdate}{JDBCConnection,character}(conn, statement,
  parameters = list(), ...)
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{statement}{the statement to send over the
  connection}

  \item{parameters}{a list of statment parameters}

  \item{...}{Ignored. Needed for compatiblity with
  generic.}
}
\description{
Execute a SQL statement on a database connection to update
the database
}

