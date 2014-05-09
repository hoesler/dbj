#' Generics for getting description of table columns available in the specified catalog.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbGetFields", function(conn, ...) standardGeneric("dbGetFields"))

#' Generics for getting a description of the tables available in the given catalog.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbGetTables", function(conn, ...) standardGeneric("dbGetTables"))

#' Generics for sending an update query.
#' 
#' @param conn An \code{DBIConnection} object.
#' @param statement the stament to send
#' @param ... Other arguments used by methods
#' @export
setGeneric("dbSendUpdate", function(conn, statement, ...) standardGeneric("dbSendUpdate"))