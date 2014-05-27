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
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  md <- get_meta_data(j_result_set)
  pull <- create_result_pull(j_result_set)
  new("JDBCResult", jr = j_result_set, md = md, pull = pull)
}

#' @rdname fetch-JDBCResult-numeric-method
#' @export
setMethod("fetch", signature(res = "JDBCResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' Fetch records from a previously executed query
#'
#' @param res an \code{\linkS4class{JDBCResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
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
      fetched <- fetch_resultpull(res@pull, stride, column_info) 
      chunks <- c(chunks, list(fetched))

      if (!infinite_pull || nrow(fetched) < stride) {
        break
      }

      stride <- 524288L # 512k
    }

    rbind_all(chunks)
  }
)

#' Clear a result set.
#' 
#' @param res an \code{\linkS4class{JDBCResult}} object.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbClearResult", signature(res = "JDBCResult"),
  function(res, ...) {
    jtry(.jcall(res@jr, "V", "close", check = FALSE))
    invisible(TRUE)
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

#' Get the names or labels for the columns of the result set.
#' 
#' @param conn an \code{\linkS4class{JDBCResult}} object.
#' @param  name Ignored. Needed for compatiblity with generic.
#' @param  use_labels if the the method should return the labels or the names of the columns
#' @param  ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbListFields", signature(conn = "JDBCResult", name = "missing"),
  function(conn, name, use_labels = TRUE, ...) {
    cols <- jtry(.jcall(conn@md, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(character())
    }
    method_name <- if (use_labels) "getColumnLabel" else "getColumnName"
    sapply(seq(cols), function(column_index) {
      jtry(.jcall(conn@md, "S", method_name, column_index, check = FALSE))
    })
  },
  valueClass = "character"
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