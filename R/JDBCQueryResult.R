#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' Class JDBCQueryResult with factory method JDBCQueryResult.
#'
#' @name JDBCQueryResult-class
#' @docType class
#' @rdname JDBCQueryResult-class
#' @export
setClass("JDBCQueryResult",
  contains = c("JDBCResult"),
  slots = c(
    statement = "character",
    j_result_set = "jobjRef",
    j_result_set_meta = "jobjRef",
    j_result_pull = "jobjRef"),
  validity = function(object) {
    if (is.jnull(object@j_result_set)) return("j_result_set is null")
    if (is.jnull(object@j_result_set_meta)) return("j_result_set_meta is null")
    if (is.jnull(object@j_result_pull)) return("j_result_pull is null")
    TRUE
  }
)

#' @param j_result_set a \code{\linkS4class{jobjRef}} object which holds a reference to a \code{java/sql/ResultSet} Java object.
#' @param statement the stament that was used to create the j_result_set
#' @return a new JDBCQueryResult object
#' @rdname JDBCDriver-class
#' @export
JDBCQueryResult <- function(j_result_set, statement = "") {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  j_result_set_meta <- get_meta_data(j_result_set)
  j_result_pull <- create_result_pull(j_result_set)
  new("JDBCQueryResult",
    j_result_set = j_result_set,
    j_result_set_meta = j_result_set_meta,
    j_result_pull = j_result_pull,
    statement = statement)
}

#' @rdname fetch-JDBCQueryResult-numeric-method
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' Fetch records from a previously executed query
#'
#' @param res an \code{\linkS4class{JDBCQueryResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "numeric"),
  function(res, n, ...) {
    cols <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(NULL)
    }
       
    column_info <- as.data.frame(t(sapply(seq(cols), function(column_index) {     
      ct <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnType", column_index, check = FALSE))
      if (ct == -5 | ct == -6 | (ct >= 2 & ct <= 8)) {
        type <- "numeric"
      } else {
        type <- "character"
      }      
      label <- jtry(.jcall(res@j_result_set_meta, "S", "getColumnLabel", column_index, check = FALSE))
      list(label = label, type = type)
    })))

    infinite_pull <- (n == -1)
    stride <- if (n == -1) {
        32768L  ## infinite j_result_pull: start fairly small to support tiny queries and increase later
      } else {
        n
      }

    chunks <- list()
    repeat {    
      fetched <- fetch_resultpull(res@j_result_pull, stride, column_info) 
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
#' @param res an \code{\linkS4class{JDBCQueryResult}} object.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbClearResult", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    j_statement <- jtry(.jcall(res@j_result_set, "Ljava/sql/Statement;", "getStatement", check = FALSE))
    if (!is.jnull(j_statement)) {
      close_statement(j_statement)
    } else {
      close_result_set(res@j_result_set)
    }
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Get info about the result set data types.
#' 
#' @param res an \code{\linkS4class{JDBCQueryResult}} object.
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    cols <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    l <- list(field.name = character(), field.type = character(), data.type = character())
    if (cols < 1) return(as.data.frame(l))
    for (i in 1:cols) {
      l$field.name[i] <- jtry(.jcall(res@j_result_set_meta, "S", "getColumnLabel", i, check = FALSE))
      l$field.type[i] <- jtry(.jcall(res@j_result_set_meta, "S", "getColumnTypeName", i, check = FALSE))
      ct <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnType", i, check = FALSE))
      l$data.type[i] <- if (ct == -5 | ct == -6 | (ct >= 2 & ct <= 8)) "numeric" else "character"
    }
    as.data.frame(l, row.names = 1:cols)    
  },
  valueClass = "data.frame"
)

#' @export
setMethod("dbGetException", signature(conn = "JDBCQueryResult"),
  function(conn, ...) {
    .NotYetImplemented()
  }
)

#' Get number of rows fetched so far
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    jtry(.jcall(res@j_result_set, "I", "getRow", check = FALSE))
  },
  valueClass = "numeric"
)

#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    .NotYetImplemented()
  },
  valueClass = "numeric"
)

#' @export
setMethod("dbGetStatement", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    res@statement
  },
  valueClass = "character"
)

#' @export
setMethod("dbHasCompleted", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    if (jtry(.jcall(res@j_result_set, "I", "getRow", check = FALSE)) > 0) {
      completed <- jtry(.jcall(res@j_result_set, "Z", "isAfterLast", check = FALSE))
    } else {      
      completed <- jtry(.jcall(res@j_result_set, "Z", "isBeforeFirst", check = FALSE)) == FALSE
      # true if the cursor is before the first row; false if the cursor is at any other position or the result set contains no rows
    }

    return(completed)
  }
)

#' Get the names or labels for the columns of the result set.
#' 
#' @param conn an \code{\linkS4class{JDBCQueryResult}} object.
#' @param  name Ignored. Needed for compatiblity with generic.
#' @param  use_labels if the the method should return the labels or the names of the columns
#' @param  ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbListFields", signature(conn = "JDBCQueryResult", name = "missing"),
  function(conn, name, use_labels = TRUE, ...) {
    cols <- jtry(.jcall(conn@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(character())
    }
    method_name <- if (use_labels) "getColumnLabel" else "getColumnName"
    sapply(seq(cols), function(column_index) {
      jtry(.jcall(conn@j_result_set_meta, "S", method_name, column_index, check = FALSE))
    })
  },
  valueClass = "character"
)

#' @export
setMethod("summary", "JDBCQueryResult",
  function(object, ...) {
    info <- result_info(object)
    cat("JDBC Result Set\n")
    cat(sprintf("  Columns: %s\n", info$cols))
    cat(sprintf("  Rows fetched: %s\n", info$rows_fetched))
  }
)

#' Get info about the result.
#' 
#' @param dbObj an object of class \code{\linkS4class{JDBCQueryResult}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCQueryResult"),
  function(dbObj, ...) {
    result_info(dbObj)
  },
  valueClass = "list"
)

result_info <- function(result_set) {
  assert_that(result_set, is_a("JDBCQueryResult"))
  list(
    cols = jtry(.jcall(result_set@j_result_set_meta, "I", "getColumnCount", check = FALSE)),
    rows_fetched = jtry(.jcall(result_set@j_result_set, "I", "getRow", check = FALSE))
  )
}

#' @export
setMethod("coerce", signature(from = "JDBCConnection", to = "JDBCQueryResult"),
  function(from, to) {
    .NotYetImplemented() # TODO: I have no idea what the api wants us to do here
  }
)