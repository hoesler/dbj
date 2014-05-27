\name{dbSendUpdate}
\alias{dbSendUpdate}
\title{Generics for sending an update query.}
\usage{
dbSendUpdate(conn, statement, parameters, ...)
}
\arguments{
  \item{conn}{An \code{DBIConnection} object.}

  \item{statement}{the statement to send}

  \item{parameters}{Optional. Either a named list or a
  data.frame of statment parameters.  A data.frame will
  produce a batch update for each row.}

  \item{...}{Other arguments used by methods}
}
\description{
Generics for sending an update query.
}

