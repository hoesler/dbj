\docType{class}
\name{JDBCDriver-class}
\alias{JDBC}
\alias{JDBCDriver-class}
\alias{JDBCResult}
\title{Class JDBCDriver with factory method JDBC.}
\usage{
JDBC(driverClass = "", classPath = "", identifier.quote = NA)

JDBCResult(j_result_set)
}
\arguments{
  \item{driverClass}{the java class name of the JDBC driver
  to use}

  \item{classPath}{a string of paths seperated by : which
  shoul get added to the classpath (see
  \link[rJava]{.jaddClassPath})}

  \item{identifier.quote}{the quoting character to quote
  identifiers}

  \item{j_result_set}{a \code{\linkS4class{jobjRef}} object
  which holds a reference to a \code{java/sql/ResultSet}
  Java object.}
}
\value{
a new JDBCResult object
}
\description{
Class JDBCDriver with factory method JDBC.
}

