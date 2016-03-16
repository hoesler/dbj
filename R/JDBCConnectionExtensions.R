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

#' Generics for sending an update query.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object.
#' @param statement the statement to send
#' @param parameters Optional. Either a named list or a data.frame of statment parameters.
#'   A data.frame will produce a batch update for each row.
#' @param ... Other arguments used by methods
#' @keywords internal
#' @export
setGeneric("dbSendUpdate",
  function(conn, statement, parameters, ...) standardGeneric("dbSendUpdate"),
  valueClass = "logical"
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
#' @export
setGeneric("dbSQLDialect",
  function(conn, ...) standardGeneric("dbSQLDialect"),
  valueClass = "sql_dialect"
)
