#' Generics for getting a description of table columns available in the specified catalog.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbGetFields",
  function(conn, ...) standardGeneric("dbGetFields"),
  valueClass = "data.frame"
)

#' Generics for getting a description of the tables available in the given catalog.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbGetTables",
  function(conn, ...) standardGeneric("dbGetTables"),
  valueClass = "data.frame"
)

#' Generics for sending an update query.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param statement the statement to send
#' @param parameters Optional. Either a named list or a data.frame of statment parameters.
#'   A data.frame will produce a batch update for each row.
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbSendUpdate",
  function(conn, statement, parameters, ...) standardGeneric("dbSendUpdate"),
  valueClass = "logical"
)