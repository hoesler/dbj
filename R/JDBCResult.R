#' @include JDBCObject.R
NULL

#' JDBCResult class.
#' 
#' Base class for \code{linkS4class{JDBCQueryResult}} and \code{linkS4class{JDBCUpdateResult}}.
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
