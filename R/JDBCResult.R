#' @include JDBCObject.R
NULL

#' Class JDBCResult
#'
#' @name JDBCResult-class
#' @rdname JDBCResult-class
#' @exportClass JDBCResult
setClass("JDBCResult", contains = c("DBIResult", "JDBCObject", "VIRTUAL"))