#' @include JDBCObject.R
NULL

#' Class JDBCResult
#'
#' @name JDBCResult-class
#' @rdname JDBCResult-class
#' @exportClass JDBCResult
setClass("JDBCResult",
  contains = c("DBIResult", "JDBCObject", "VIRTUAL"),
  slots = c(
    connection = "JDBCConnection"))

setMethod("dbGetDriver", signature(dbObj = "JDBCResult"),
  function(dbObj, ...) {
    dbGetDriver(dbObj@connection)
  }
)

#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCResult"),
  function(dbObj, ...) {
    SQLKeywords(dbObj@connection)
  },
  valueClass = "character"
)