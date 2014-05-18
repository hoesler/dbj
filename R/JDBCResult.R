#' @include JDBCObject.R
#' @include JavaUtils.R
NULL

#' Class JDBCResult with factory method JDBCResult.
#'
#' @name JDBCResult-class
#' @docType class
#' @rdname JDBCResult-class
#' @export
setClass("JDBCResult",
  contains = c("DBIResult", "JDBCObject"),
  slots = c(
    jr = "jobjRef",
    md = "jobjRef",
    pull = "jobjRef"),
  validity = function(object) {
    if (is.jnull(object@jr)) return("jr is null")
    if (is.jnull(object@md)) return("md is null")
    if (is.jnull(object@pull)) return("pull is null")
    TRUE
  }
)

#' @param j_result_set a \code{\linkS4class{jobjRef}} object which holds a reference to a \code{java/sql/ResultSet} Java object.
#' @return a new JDBCResult object
#' @rdname JDBCDriver-class
#' @export
JDBCResult <- function(j_result_set) {
  assert_that(is(j_result_set, "jobjRef"), j_result_set@jclass %instanceof% "java.sql.ResultSet")

  md <- getMetaData(j_result_set)
  pull <- getResultPull(j_result_set)
  new("JDBCResult", jr = j_result_set, md = md, pull = pull)
}

getMetaData <- function(j_result_set) {
  verifyNotNull(j_result_set)
  j_meta_data <- jtry(.jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE))
  verifyNotNull(j_meta_data, "Unable to retrieve JDBC result set meta data for ", j_result_set, " in dbSendQuery")
  j_meta_data
}

getResultPull <- function(j_result_set) {
  verifyNotNull(j_result_set)
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
    cols <- jtry(.jcall(res@md, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(NULL)
    }
       
    column_info <- as.data.frame(t(sapply(seq(cols), function(column_index) {     
      ct <- jtry(.jcall(res@md, "I", "getColumnType", column_index, check = FALSE))
      if (ct == -5 | ct == -6 | (ct >= 2 & ct <= 8)) {
        type <- "numeric"
      } else {
        type <- "character"
      }      
      label <- jtry(.jcall(res@md, "S", "getColumnLabel", column_index, check = FALSE))
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
  java_table <- jtry(.jcall(res@pull, "Linfo/urbanek/Rpackage/RJDBC/Table;", "fetch", as.integer(rows), check = FALSE))
  verifyNotNull(java_table, "Table creation failed")
  column_count <- jtry(.jcall(java_table, "I", "columnCount", check = FALSE))
  row_count <- jtry(.jcall(java_table, "I", "rowCount", check = FALSE))

  column_list <- lapply(seq(column_count), function(column_index) {
    column <- jtry(.jcall(java_table, "Linfo/urbanek/Rpackage/RJDBC/Column;", "getColumn", as.integer(column_index - 1), check = FALSE))

    column_data <- c()
    if (column_info[column_index, "type"] == "numeric") {
      column_data <- jtry(.jcall(column, "[D", "toDoubleArray", check = FALSE))
    } else {
      column_data <- jtry(.jcall(column, "[Ljava/lang/String;", "toStringArray", check = FALSE))     
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
    jtry(.jcall(res@jr, "V", "close", check = FALSE))
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
    cols <- jtry(.jcall(res@md, "I", "getColumnCount", check = FALSE))
    l <- list(field.name = character(), field.type = character(), data.type = character())
    if (cols < 1) return(as.data.frame(l))
    for (i in 1:cols) {
      l$field.name[i] <- jtry(.jcall(res@md, "S", "getColumnLabel", i, check = FALSE))
      l$field.type[i] <- jtry(.jcall(res@md, "S", "getColumnTypeName", i, check = FALSE))
      ct <- jtry(.jcall(res@md, "I", "getColumnType", i, check = FALSE))
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
    if (jtry(.jcall(res@jr, "I", "getRow", check = FALSE)) > 0) {
      completed <- jtry(.jcall(res@jr, "Z", "isAfterLast", check = FALSE))
    } else {      
      completed <- jtry(.jcall(res@jr, "Z", "isBeforeFirst", check = FALSE)) == FALSE
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