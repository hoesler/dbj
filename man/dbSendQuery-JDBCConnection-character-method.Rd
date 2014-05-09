\docType{methods}
\name{dbSendQuery,JDBCConnection,character-method}
\alias{dbSendQuery,JDBCConnection,character-method}
\title{Execute a SQL statement on a database connection}
\usage{
\S4method{dbSendQuery}{JDBCConnection,character}(conn, statement, ...,
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
Execute a SQL statement on a database connection
}

