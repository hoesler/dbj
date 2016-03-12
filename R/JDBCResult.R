#' @include JDBCObject.R
NULL

#' JDBCResult class.
#' 
#' Base class for \code{linkS4class{JDBCQueryResult}} and \code{linkS4class{JDBCUpdateResult}}.
#'
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

RESULT_SET_TYPE <- list(
  TYPE_FORWARD_ONLY = 1003L,
  TYPE_SCROLL_INSENSITIVE = 1004L,
  TYPE_SCROLL_SENSITIVE = 1005L
)

RESULT_SET_CONCURRENCY <- list(
  CONCUR_READ_ONLY = 1007L,
  CONCUR_UPDATABLE = 1008L
)
