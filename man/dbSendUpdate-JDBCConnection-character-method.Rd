\docType{methods}
\name{dbSendUpdate,JDBCConnection,character-method}
\alias{dbSendUpdate,JDBCConnection,character-method}
\title{Execute a SQL statement on a database connection to update the database}
\usage{
\S4method{dbSendUpdate}{JDBCConnection,character}(conn, statement, ...,
  list = NULL)
}
\arguments{
  \item{conn}{An existing
  \code{\linkS4class{JDBCConnection}}}

  \item{statement}{the statement to send over the
  connection}

  \item{...}{statment parameters}

  \item{list}{a list of statment parameters}
}
\description{
Execute a SQL statement on a database connection to update
the database
}

