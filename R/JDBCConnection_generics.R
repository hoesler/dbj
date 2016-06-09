#' Generics for getting a description of table columns available in the specified catalog.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object.
#' @param ... Other arguments used by methods
#' @keywords internal
#' @export
setGeneric("dbGetFields",
  function(conn, ...) standardGeneric("dbGetFields"),
  valueClass = "data.frame"
)

#' Generics for getting a description of the tables available in the given catalog.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object.
#' @param ... Other arguments used by methods
#' @keywords internal
#' @export
setGeneric("dbGetTables",
  function(conn, ...) standardGeneric("dbGetTables"),
  valueClass = "data.frame"
)

#' Truncate a table
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object.
#' @param name The name of the table.
#' @param ... Other arguments used by methods.
#' @keywords internal
#' @export
setGeneric("dbTruncateTable",
  function(conn, name, ...) standardGeneric("dbTruncateTable"),
  valueClass = "logical"
)

#' Get the current SQL dialect of the connection
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object.
#' @param ... Other arguments used by methods
#' @keywords internal
#' @export
setGeneric("dbSQLDialect",
  function(conn, ...) standardGeneric("dbSQLDialect"),
  valueClass = "sql_dialect"
)

#' Send an update query.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param statement A SQL statement to send over the connection. Use \code{?} for input parameters.
#' @param parameters A list of statement parameters, which will be inserted in order.
#' @param ... Other arguments passed on to methods.
#' @return A logical indicating success
#' @keywords internal
#' @export
setGeneric("dbSendUpdate",
  function(conn, statement, parameters, ...) standardGeneric("dbSendUpdate"),
  valueClass = "logical"
)
