#' @include JDBCObjectExtensions.R
NULL

#' Class JDBCObject
#'
#' @export
setClass("JDBCObject", contains = c("DBIObject", "VIRTUAL"))

#' @rdname JDBCObject-class
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

#' @rdname JDBCObject-class
#' @param obj An R object whose SQL type we want to determine.
setMethod("dbDataType", signature(dbObj = "JDBCObject"),
  function(dbObj, obj, ...) {
    drv <- dbGetDriver(dbObj)
    assert_that(is(drv, "JDBCDriver"))
    dbDataType(drv, obj)
  },
  valueClass = "character"
)

#' @rdname JDBCObject-class
#' @param object an object of class \code{\linkS4class{JDBCObject}}
#' @export
setMethod("summary", signature(object = "JDBCObject"),
  function(object, ...) {
    stop(sprintf("summary is not implemented for class %s", class(object)))
  }
)

#' @rdname JDBCObject-class
#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    warning("The full list of SQL keywords cannot be fetched from this object. Use a JDBCConnection object instead.")
    callNextMethod()
  }
)

#' @rdname JDBCObject-class
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    stop(sprintf("dbIsValid is not implemented for class %s", class(dbObj)))
  },
  valueClass = "logical"
)
