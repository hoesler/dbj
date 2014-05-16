#' @import dplyr
#' @include JDBCObject.R
NULL

#' Class JDBCResult
#'
#' @name JDBCResult-class
#' @rdname JDBCResult-class
#' @exportClass JDBCResult
setClass("JDBCResult",
  contains = c("DBIResult", "JDBCObject"),
  slots = c(
    jr = "jobjRef",
    md = "jobjRef",
    pull = "jobjRef"))

setMethod("initialize", signature(.Object = "JDBCResult"),
  function(.Object, ...) {
    .Object <- callNextMethod()
    
    if (is.null(.Object@jr)) {
      stop("Java result is null")
    }

    .Object@md <- getMetaData(.Object@jr)
    .Object@pull <- getResultPull(.Object@jr)

    .Object
  }
)

getMetaData <- function(j_result_set) {
  j_meta_data <- .jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE)
  verifyNotNull(j_meta_data, "Unable to retrieve JDBC result set meta data for ", j_result_set, " in dbSendQuery")
  j_meta_data
}

getResultPull <- function(j_result_set) {
  rp <- .jnew("info/urbanek/Rpackage/RJDBC/JDBCResultPull", .jcast(j_result_set, "java/sql/ResultSet"), check = FALSE)
  verifyNotNull(rp, "Failed to instantiate JDBCResultPull hepler object")
  rp
}

setMethod("fetch", signature(res = "JDBCResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' Fetch records from a previously executed query
#'
#' @param res an \code{\linkS4class{JDBCResult}} object.
#' @param n maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("fetch", signature(res = "JDBCResult", n = "numeric"),
  function(res, n, ...) {
    cols <- .jcall(res@md, "I", "getColumnCount")
    if (cols < 1L) {
      return(NULL)
    }
       
    column_info <- as.data.frame(t(sapply(seq(cols), function(column_index) {     
      ct <- .jcall(res@md, "I", "getColumnType", column_index)
      if (ct == -5 | ct == -6 | (ct >= 2 & ct <= 8)) {
        type <- "numeric"
      } else {
        type <- "character"
      }      
      label <- .jcall(res@md, "S", "getColumnLabel", column_index)
      list(label = label, type = type)
    })))

    infinite_pull <- (n == -1)
    stride <- if (n == -1) {
        32768L  ## infinite pull: start fairly small to support tiny queries and increase later
      } else {
        n
      }

    chunks <- list()
    repeat {    
      fetched <- fetch_resultpull(res, stride, column_info) 
      chunks <- c(chunks, list(fetched))

      if (!infinite_pull || nrow(fetched) < stride) {
        break
      }

      stride <- 524288L # 512k
    }

    rbind_all(chunks)
  }
)

fetch_resultpull <- function(res, rows, column_info) {
  java_table <- .jcall(res@pull, "Linfo/urbanek/Rpackage/RJDBC/Table;", "fetch", as.integer(rows))
  verifyNotNull(java_table, "Table creation failed")
  column_count <- .jcall(java_table, "I", "columnCount")
  row_count <- .jcall(java_table, "I", "rowCount")

  column_list <- lapply(seq(column_count), function(column_index) {
    column <- .jcall(java_table, "Linfo/urbanek/Rpackage/RJDBC/Column;", "getColumn", as.integer(column_index - 1))

    column_data <- c()
    if (column_info[column_index, "type"] == "numeric") {
      column_data <- .jcall(column, "[D", "toDoubleArray")
    } else {
      column_data <- .jcall(column, "[Ljava/lang/String;", "toStringArray")     
    }

    column_data
  })

  # as.data.frame is expensive - create it on the fly from the list
  attr(column_list, "row.names") <- c(NA_integer_, row_count)
  class(column_list) <- "data.frame"
  names(column_list) <- column_info$label
  column_list
}

#' Clear a result set.
#' 
#' @param res an \code{\linkS4class{JDBCResult}} object.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbClearResult", signature(res = "JDBCResult"),
  function(res, ...) {
    .jcall(res@jr, "V", "close");
    TRUE
  },
  valueClass = "logical"
)

#' Get info about the result.
#' 
#' @param dbObj an object of class \code{\linkS4class{JDBCResult}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCResult"),
  function(dbObj, ...) {
    list()
  },
  valueClass = "list"
)

#' Get info about the result set data types.
#' 
#' @param res an \code{\linkS4class{JDBCResult}} object.
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCResult"),
  function(res, ...) {
    cols <- .jcall(res@md, "I", "getColumnCount")
    l <- list(field.name = character(), field.type = character(), data.type = character())
    if (cols < 1) return(as.data.frame(l))
    for (i in 1:cols) {
      l$field.name[i] <- .jcall(res@md, "S", "getColumnLabel", i)
      l$field.type[i] <- .jcall(res@md, "S", "getColumnTypeName", i)
      ct <- .jcall(res@md, "I", "getColumnType", i)
      l$data.type[i] <- if (ct == -5 | ct == -6 | (ct >= 2 & ct <= 8)) "numeric" else "character"
    }
    as.data.frame(l, row.names = 1:cols)    
  },
  valueClass = "data.frame"
)

#' @export
setMethod("dbGetException", signature(conn = "JDBCResult"),
  function(conn, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("dbGetRowCount", signature(res = "JDBCResult"),
  function(res, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCResult"),
  function(res, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("dbGetStatement", signature(res = "JDBCResult"),
  function(res, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("dbHasCompleted", signature(res = "JDBCResult"),
  function(res, ...) {
    if (.jcall(res@jr, "I", "getRow") > 0) {
      completed <- .jcall(res@jr, "Z", "isAfterLast")
    } else {      
      completed <- .jcall(res@jr, "Z", "isBeforeFirst") == FALSE
      # true if the cursor is before the first row; false if the cursor is at any other position or the result set contains no rows
    }

    return(completed)
  }
)

#' @export
setMethod("dbListFields", signature(conn = "JDBCResult", name = "missing"),
  function(conn, name, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("summary", "JDBCResult",
  function(object, ...) {
    .NotYetImplemented()
  }
)

#' @export
setMethod("coerce", signature(from = "JDBCConnection", to = "JDBCResult"),
  function(from, to) {
    .NotYetImplemented()
  }
)