#' @include JDBCObject.R
NULL

#' Class JDBCResult
#'
#' @keywords internal
#' @export
setClass("JDBCResult",
  contains = c("DBIResult", "JDBCObject", "VIRTUAL"),
  slots = c(
    connection = "JDBCConnection"))

#' @rdname JDBCResult-class
#' @param dbObj An object of class \code{\linkS4class{JDBCResult}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbGetDriver", signature(dbObj = "JDBCResult"),
  function(dbObj, ...) {
    dbGetDriver(dbObj@connection)
  }
)

#' @rdname JDBCResult-class
#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCResult"),
  function(dbObj, ...) {
    SQLKeywords(dbObj@connection)
  },
  valueClass = "character"
)