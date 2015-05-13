#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' Class JDBCQueryResult with factory method JDBCQueryResult.
#'
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
#' @param connection the \code{\linkS4class{JDBCConnection}} which which was used for the query.
#' @param statement the stament that was used to create the j_result_set
#' @return a new JDBCQueryResult object
#' @rdname JDBCQueryResult-class
#' @export
JDBCQueryResult <- function(j_result_set, connection, statement = "") {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  assert_that(is(connection, "JDBCConnection"))

  j_result_set_meta <- get_meta_data(j_result_set)
  j_result_pull <- create_result_pull(j_result_set)
  new("JDBCQueryResult",
    j_result_set = j_result_set,
    j_result_set_meta = j_result_set_meta,
    j_result_pull = j_result_pull,
    statement = statement,
    connection = connection)
}

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
       
    column_info <- dbColumnInfo(res, c("label", "nullable", "sql_type"))

    infinite_pull <- (n == -1)
    stride <- if (infinite_pull) {
      32768L  #start fairly small to support tiny queries and increase later
      # TODO does the increaing stride srategy really has any performnace / memory benefit?
    } else {
      n
    }

    chunks <- list()
    repeat {    
      fetched <- fetch_resultpull(res@j_result_pull, stride, column_info, dbGetDriver(res)@read_conversions) 
      chunks <- c(chunks, list(fetched))

      if (!infinite_pull || nrow(fetched) < stride) {
        break
      }

      stride <- 524288L # 512k
    }

    do.call(rbind, chunks)
  }
)

#' @rdname fetch-JDBCQueryResult-numeric-method
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' @rdname JDBCQueryResult-class
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
#' @param what a character vector indicating which info to return.
#'   Expected is a subset of \code{c("label", "sql_type", "type_name", "nullable")}.
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCQueryResult"),
  function(res, what = c("label", "sql_type", "type_name", "nullable"), ...) {
    assert_that(is.character(what))

    if (length(what) == 0) {
      return(data.frame())
    }
    
    column_count <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    
    column_info <- list()

    if ("label" %in% what) {
      column_info <- c(column_info, list(label = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "S", "getColumnLabel", i, check = FALSE))
      }, "")))
    }

    if ("sql_type" %in% what) {
      column_info <- c(column_info, list(sql_type = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "I", "getColumnType", i, check = FALSE)) 
      }, as.integer(0))))
    }

    if ("type_name" %in% what) {
      column_info <- c(column_info, list(type_name = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "S", "getColumnTypeName", i, check = FALSE))
      }, "")))
    }
    
    if ("nullable" %in% what) {
      column_info <- c(column_info, list(nullable = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "I", "isNullable", i, check = FALSE)) # 0 = disallows NULL, 1 = allows NULL, 2 = unknown
      }, as.integer(0))))
    }

    as.data.frame(column_info, row.names = seq(column_count))   
  },
  valueClass = "data.frame"
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    jtry(.jcall(res@j_result_set, "I", "getRow", check = FALSE))
  },
  valueClass = "numeric"
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    .NotYetImplemented()
  },
  valueClass = "numeric"
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbGetStatement", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    res@statement
  },
  valueClass = "character"
)

#' @rdname JDBCQueryResult-class
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

#' @rdname JDBCQueryResult-class
#' @param object an \code{\linkS4class{JDBCUpdateResult}} object.
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
  assert_that(is(result_set, "JDBCQueryResult"))
  list(
    cols = jtry(.jcall(result_set@j_result_set_meta, "I", "getColumnCount", check = FALSE)),
    rows_fetched = jtry(.jcall(result_set@j_result_set, "I", "getRow", check = FALSE))
  )
}
