\docType{methods}
\name{dbConnect,JDBCDriver-method}
\alias{dbConnect,JDBCDriver-method}
\title{Create a JDBC connection.}
\usage{
\S4method{dbConnect}{JDBCDriver}(drv, url, user = "", password = "", ...)
}
\arguments{
  \item{url}{the url to connect to}

  \item{user}{the user to log in}

  \item{password}{the users password}

  \item{drv}{ an object that inherits from
  \code{DBIDriver}, a character string specifying the DBMS
  driver, e.g., "RPgSQL", "ROracle", "Informix", or
  possibly another \code{dbConnect} object.  }

  \item{...}{ authorization arguments needed by the DBMS
  instance; these typically include \code{user},
  \code{password}, \code{dbname}, \code{host}, \code{port},
  etc.  For details see the appropriate \code{DBIDriver}.
  }
}
\description{
Create a JDBC connection.
}

