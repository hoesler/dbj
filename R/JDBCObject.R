#' Class JDBCObject
#'
#' @name JDBCObject-class
#' @rdname JDBCObject-class
#' @exportClass JDBCObject
setClass("JDBCObject", contains = c("DBIObject", "VIRTUAL"))

#' Get metadata about a database object.
#'
#' @param dbObj An object of class \code{\linkS4class{JDBCObject}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    list(
      name = "JDBC",
      driver.version = "0.1-1",
      DBI.version = "0.2-7",
      client.version = NA,
      max.connections = NA)
  }
)

#' Determine the SQL Data Type of an R object.
#'
#' @docType methods
#' @param dbObj a \code{\linkS4class{JDBCObject}} object.
#' @param obj an R object whose SQL type we want to determine.
#' @param ... Needed for compatibility with generic. Otherwise ignored.
#' @export
setMethod("dbDataType", signature(dbObj = "JDBCObject"),
  function(dbObj, obj, ...) {
    if (is.integer(obj)) "INTEGER"
    else if (is.numeric(obj)) "DOUBLE PRECISION"
    else "VARCHAR(255)"
  },
  valueClass = "character"
)


#' @export
setMethod("summary", signature(object = "JDBCObject"),
  function(object, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("isSQLKeyword", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    callNextMethod()
  }
)

#' @export
setMethod("make.db.names", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    callNextMethod()
  }
)
