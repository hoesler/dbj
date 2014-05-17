\docType{methods}
\name{dbConnect,JDBCDriver-method}
\alias{dbConnect,JDBCDriver-method}
\title{Create a JDBC connection.}
\usage{
\S4method{dbConnect}{JDBCDriver}(drv, url, user = "", password = "", ...)
}
\arguments{
  \item{drv}{An object of class
  \code{\linkS4class{JDBCDriver}}}

  \item{url}{the url to connect to}

  \item{user}{the user to log in}

  \item{password}{the users password}

  \item{...}{ authorization arguments needed by the DBMS
  instance; these typically include \code{user},
  \code{password}, \code{dbname}, \code{host}, \code{port},
  etc.  For details see the appropriate \code{DBIDriver}.
  }
}
\description{
Create a JDBC connection.
}

