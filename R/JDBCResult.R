#' @include JDBCObject.R
NULL

#' JDBCResult class.
#' 
#' Base class for \code{linkS4class{JDBCQueryResult}} and \code{linkS4class{JDBCUpdateResult}}.
#' @param res A \code{\linkS4class{JDBCResult}} object.
#' @param params A list of bindings.
#' @param ... Ignored. Needed for compatibility with generic.
#'
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
#' @section Methods:
#' \code{dbBind}: Unsupported. Use parameter binding of dbSendQuery-JDBCConnection-character-method instead.
#' @export
setMethod("dbBind", signature(res = "JDBCResult"),
  function(res, params, ...) {
    stop("Unsupported. Use parameter binding of dbSendQuery instead.")
  }
)
