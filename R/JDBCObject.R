#' @include JDBCObjectExtensions.R
NULL

#' Class JDBCObject
#'
#' @keywords internal
#' @export
setClass("JDBCObject", contains = c("DBIObject", "VIRTUAL"))

#' @describeIn JDBCObject Unless overwritten, this method will \code{stop} with an error.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    stop(sprintf("dbGetInfo is not implemented for class %s", class(dbObj)))
  },
  valueClass = "list"
)

#' @describeIn JDBCObject the method is forwarded to the \code{JDBCDriver} object returnd by \code{dbGetDriver(dbObj)}.
#' @export
setMethod("dbDataType", signature(dbObj = "JDBCObject"),
  function(dbObj, obj, ...) {
    drv <- dbGetDriver(dbObj)
    assert_that(is(drv, "JDBCDriver"))
    dbDataType(drv, obj)
  },
  valueClass = "character"
)

#' @describeIn JDBCObject to get the full list of keywords you must pass a \code{\linkS4class{JDBCConnection}} object. For all other objects not overwriting this method it will forward to the superclass with a warning. 
#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    warning("The full list of SQL keywords cannot be fetched from this object. Use a JDBCConnection object instead.")
    callNextMethod()
  }
)

#' @describeIn JDBCObject Unless overwritten, this method will \code{stop} with an error.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    stop(sprintf("dbIsValid is not implemented for class %s", class(dbObj)))
  },
  valueClass = "logical"
)
