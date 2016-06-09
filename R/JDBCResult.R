#' @include JDBCObject.R
NULL

#' JDBCResult class
#' 
#' Base class for \code{\linkS4class{JDBCQueryResult}} and \code{\linkS4class{JDBCUpdateResult}}.
#' 
#' @family result classes
#' @export
setClass("JDBCResult",
  contains = c("DBIResult", "JDBCObject", "VIRTUAL"))

RESULT_SET_TYPE <- list(
  TYPE_FORWARD_ONLY = 1003L,
  TYPE_SCROLL_INSENSITIVE = 1004L,
  TYPE_SCROLL_SENSITIVE = 1005L
)

RESULT_SET_CONCURRENCY <- list(
  CONCUR_READ_ONLY = 1007L,
  CONCUR_UPDATABLE = 1008L
)

#' @rdname JDBCResult-class
#' @aliases dbBind,JDBCResult-method
#' @param res An object inheriting from \code{\linkS4class{JDBCResult}}
#' @inheritParams DBI::dbBind
#' @section Methods:
#' \code{dbBind}: Unsupported. Use parameter binding of \code{\link{dbSendQuery,JDBCConnection,character-method}} instead.
#' @export
setMethod("dbBind", signature(res = "JDBCResult"),
  function(res, params, ...) {
    stop("Unsupported. Use parameter binding of dbSendQuery instead.")
  }
)

#' @rdname JDBCResult-class
#' @aliases fetch,JDBCResult-method
#' @inheritParams DBI::fetch
#' @section Methods:
#' \code{fetch}: Forwards to \code{\link{dbFetch}}.
#' @export
setMethod("fetch", signature(res = "JDBCResult"), function(res, n = -1, ...) {
  dbFetch(res, n = n, ...)
})
