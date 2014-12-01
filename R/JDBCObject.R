#' @include JDBCObjectExtensions.R
NULL

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
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbDataType", signature(dbObj = "JDBCObject"),
  function(dbObj, obj, ...) {
    drv <- dbGetDriver(dbObj)
    assert_that(is(drv, "JDBCDriver"))
    dbDataType(drv, obj)
  },
  valueClass = "character"
)

#' @export
setMethod("summary", signature(object = "JDBCObject"),
  function(object, ...) {
    stop(sprintf("summary is not implemented for class %s", class(dbObj)))
  }
)

#' @export
setMethod("isSQLKeyword", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    callNextMethod()
  }
)

#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    warning("The full list of SQL keywords cannot be fetched from this object. Use a JDBCConnection object instead.")
    callNextMethod()
  }
)

#' @export
setMethod("make.db.names", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    callNextMethod()
  }
)

#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    stop(sprintf("dbIsValid is not implemented for class %s", class(dbObj)))
  },
  valueClass = "logical"
)
