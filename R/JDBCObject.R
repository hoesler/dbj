#' @include JDBCObject_generics.R
NULL

#' JDBCObject class
#' 
#' @param dbObj an object of type \code{\linkS4class{JDBCObject}}
#' @keywords internal
#' @export
setClass("JDBCObject", contains = c("DBIObject", "VIRTUAL"))

#' @describeIn JDBCObject Deprecated in DBI
#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    warning("The full list of SQL keywords cannot be fetched from this object. Use a JDBCConnection object instead.")
    callNextMethod()
  }
)

#' @describeIn JDBCObject the method is forwarded to the \code{JDBCDriver} object returnd by \code{dbGetDriver(dbObj)}.
#' @export
setMethod("dbDataType", signature(dbObj = "JDBCObject"),
  function(dbObj, obj, ...) {
    drv <- dbGetDriver(dbObj)
    assert_that(is(drv, "JDBCDriver"))
    dbDataType(drv, obj)
  }
)