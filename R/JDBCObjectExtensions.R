#' Generics for getting the driver object.
#' 
#' @param dbObj A \code{JDBCObject} object.
#' @param ... Other arguments used by methods
#' @keywords internal
setGeneric("dbGetDriver",
  function(dbObj, ...) standardGeneric("dbGetDriver"),
  valueClass = "JDBCDriver"
)