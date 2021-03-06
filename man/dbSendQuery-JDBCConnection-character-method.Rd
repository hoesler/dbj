% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/JDBCConnection.R
\docType{methods}
\name{dbSendQuery,JDBCConnection,character-method}
\alias{dbSendQuery,JDBCConnection,character-method}
\title{Execute a prepared SQL statement on a database connection}
\usage{
\S4method{dbSendQuery}{JDBCConnection,character}(conn, statement,
  parameters = list())
}
\arguments{
\item{conn}{A \code{\linkS4class{JDBCConnection}} object, as produced by
\code{\link{dbConnect}}.}

\item{statement}{A character vector of length one specifying the input to a
\href{https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html}{prepared SQL statement}.}

\item{parameters}{A list of statement parameters, which will be inserted in order.}
}
\value{
A \code{\linkS4class{JDBCResult}} object.
  Depending on the query type a \code{\linkS4class{JDBCQueryResult}} or a \code{\linkS4class{JDBCUpdateResult}}.
}
\description{
Execute a prepared SQL statement on a database connection
}
\examples{
\dontrun{
dbSendQuery(con, "UPDATE EMPLOYEES SET SALARY = ? WHERE ID = ?", list(3500, 624L))
}
}
\seealso{
Other JDBCConnection methods: \code{\link{dbConnect,JDBCConnection-method}},
  \code{\link{dbDisconnect,JDBCConnection-method}},
  \code{\link{dbj-edit-tables}},
  \code{\link{dbj-sql-generate}},
  \code{\link{dbj-sql-quote}}, \code{\link{dbj-table}}
}

