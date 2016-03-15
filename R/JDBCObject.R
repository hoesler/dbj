#' @include JDBCObjectExtensions.R
NULL

#' Class JDBCObject
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
