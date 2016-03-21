#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' JDBCQueryResult class.
#' 
#' @param res,dbObj an \code{\linkS4class{JDBCQueryResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param fetch_size a hint to the number of rows that should be fetched from the database in a single block.
#'    See \url{http://docs.oracle.com/javase/7/docs/api/java/sql/Statement.html#setFetchSize(int)}.
#' @param what a character vector indicating which info to return.
#'   Expected is a subset of \code{c("name", "field.type", "data.type", "label", "nullable")}.
#' @param ... Ignored. Needed for compatibility with generic.
#' 
#' @keywords internal
#' @export
JDBCQueryResult <- setRefClass("JDBCQueryResult",
  contains = c("JDBCResult"),
  fields = list(
    row_count = "numeric",
    completed = "logical",
    statement = "character",
    j_result_set = "jobjRef",
    j_result_set_meta = "jobjRef",
    j_result_pull = "jobjRef",
    connection = "JDBCConnection"),
  methods = list(
    increaseRowCount = function(x) {
      row_count <<- row_count + x
    },
    initialize = function(j_result_set, connection, statement = "") {
      assert_that(j_result_set %instanceof% "java.sql.ResultSet")
      assert_that(is(connection, "JDBCConnection"))

      .self$j_result_set <<- j_result_set
      .self$j_result_set_meta <<- get_meta_data(j_result_set)
      .self$j_result_pull <<- create_result_pull(j_result_set)
      .self$statement <<- statement
      .self$connection <<- connection
      .self$row_count <<- 0
      .self$completed <<- FALSE
    }
  ),
  validity = function(object) {
    if (is.jnull(object$j_result_set)) return("j_result_set is null")
    if (is.jnull(object$j_result_set_meta)) return("j_result_set_meta is null")
    if (is.jnull(object$j_result_pull)) return("j_result_pull is null")
    TRUE
  }
)

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{fetch}: Fetch n results
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "numeric"),
  function(res, n, fetch_size = 0, ...) {
    assert_that(is.numeric(fetch_size))    

    cols <- jtry(.jcall(res$j_result_set_meta, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(NULL)
    }
       
    column_info <- dbColumnInfo(res, c("label", "nullable", "jdbc.type"))

    infinite_pull <- (n == -1)
    stride <- if (infinite_pull) {
      32768L  #start fairly small to support tiny queries and increase later
      # TODO does the increaing stride srategy really has any performnace / memory benefit?
    } else {
      n
    }

    chunks <- list()
    repeat {    
      fetched <- fetch_resultpull(res$j_result_pull, stride, column_info, dbGetDriver(res)@read_conversions, fetch_size) 
      chunks <- c(chunks, list(fetched))  

      if (nrow(fetched) < stride) {
        res$completed <- TRUE
      }

      if (infinite_pull) {
        if (res$completed) {
          break
        } else {
          stride <- 524288L # 512k
        }
      } else {
        break
      }
    }

    ret <- do.call(rbind, chunks)
    res$increaseRowCount(nrow(ret))

    return(ret)
  }
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbClearResult", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    closed <- jtry(.jcall(res$j_result_set, "Z", "isClosed", check = FALSE))
    if (!closed) {
      j_statement <- jtry(.jcall(res$j_result_set, "Ljava/sql/Statement;", "getStatement", check = FALSE))
      if (!is.jnull(j_statement)) {
        close_statement(j_statement)
      } else {
        close_result_set(res$j_result_set)
      }
    } else {
      warning("Result has already been closed")
    }
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @section Methods:
#' \code{dbColumnInfo}: Get info about the result set data types. Returns a data.frame with one row per output field in \code{res}.
#'   Includes \code{name}, \code{field.type} (the SQL type)
#'   and \code{data.type} (the R data type) columns for the default what.
#'   Additionally, \code{label} (The field label)
#'   and \code{nullable} (0 = disallows NULL, 1 = allows NULL, 2 = unknown) can be fetched.
#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCQueryResult"),
  function(res, what = c("name", "field.type", "data.type", "jdbc.type"), ...) {
    assert_that(is.character(what))

    if (length(what) == 0) {
      return(data.frame())
    }
    
    column_count <- jtry(.jcall(res$j_result_set_meta, "I", "getColumnCount", check = FALSE))
    
    column_info <- list()

    if ("name" %in% what) { # TODO: return lables as names?
      column_info <- c(column_info, list(name = vapply(seq(column_count), function(i) {
        jtry(.jcall(res$j_result_set_meta, "S", "getColumnName", i, check = FALSE))
      }, "")))
    }

    if ("field.type" %in% what) {
      column_info <- c(column_info, list(field.type = vapply(seq(column_count), function(i) {
        jtry(.jcall(res$j_result_set_meta, "S", "getColumnTypeName", i, check = FALSE))
      }, "")))
    }

    if ("jdbc.type" %in% what) {
      column_info <- c(column_info, list(jdbc.type = vapply(seq(column_count), function(i) {
        jtry(.jcall(res$j_result_set_meta, "I", "getColumnType", i, check = FALSE))
      }, 0L)))
    }

    if ("data.type" %in% what) {
      read_conversions <- dbGetDriver(res)@read_conversions
      column_info <- c(column_info, list(data.type = vapply(seq(column_count), function(i) {
        jdbc.type <-
        if ("jdbc.type" %in% names(column_info)) {
          column_info$jdbc.type[i]
        } else {
          jtry(.jcall(res$j_result_set_meta, "I", "getColumnType", i, check = FALSE))
        }
        read_conversions[sapply(read_conversions, function (x) { x$condition(list("jdbc.type" = jdbc.type)) })][[1]]$r_class
      }, "")))
    }

    if ("label" %in% what) {
      column_info <- c(column_info, list(label = vapply(seq(column_count), function(i) {
        jtry(.jcall(res$j_result_set_meta, "S", "getColumnLabel", i, check = FALSE))
      }, "")))
    }
    
    if ("nullable" %in% what) {
      column_info <- c(column_info, list(nullable = vapply(seq(column_count), function(i) {
        jtry(.jcall(res$j_result_set_meta, "I", "isNullable", i, check = FALSE)) # 0 = disallows NULL, 1 = allows NULL, 2 = unknown
      }, 0L)))
    }

    as.data.frame(column_info, row.names = seq(column_count), stringsAsFactors = FALSE)   
  },
  valueClass = "data.frame"
)

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{dbGetRowCount}: Count rows in result set
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    res$row_count
  },
  valueClass = "numeric"
)

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{dbGetRowsAffected}: Count rows affected by the last update query
#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    .NotYetImplemented()
  },
  valueClass = "numeric"
)

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{dbHasCompleted}: Check if all results have been fetched
#' @export
setMethod("dbHasCompleted", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    checkValid(res)
    res$completed
  }
)

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{dbGetInfo}: Get info
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
    statement = result_set$statement,
    col.count = jtry(.jcall(result_set$j_result_set_meta, "I", "getColumnCount", check = FALSE)),
    row.count = dbGetRowCount(result_set),
    has.completed = dbHasCompleted(result_set),
    is.select = TRUE,
    # TODO: total number of records to be fetched
    rows.affected = NA
  )
}

#' @rdname JDBCQueryResult-class
#' @section Methods:
#' \code{dbIsValid}: Is the result valid
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCQueryResult"),
  function(dbObj, ...) {
    closed <- jtry(.jcall(dbObj$j_result_set, "Z", "isClosed", check = FALSE))
    return(!closed)
  },
  valueClass = "logical"
)

checkValid <- function(res) {
  if (!dbIsValid(res)) {
    stop("The result set has been closed")
  }
}

#' Deprecated! Use dbColumnInfo instead. 
#' 
#' @param conn an \code{\linkS4class{JDBCQueryResult}} object.
#' @param  name Ignored. Needed for compatiblity with generic.
#' @param  ... Ignored. Needed for compatiblity with generic.
#' @keywords internal
#' @export
setMethod("dbListFields", signature(conn = "JDBCQueryResult", name = "missing"),
  function(conn, name, ...) {
    dbColumnInfo(conn, "name")$name
  },
  valueClass = "character"
)

#' @rdname JDBCQueryResult-class
#' @export
setMethod("dbGetDriver", signature(dbObj = "JDBCQueryResult"),
  function(dbObj, ...) {
    dbGetDriver(dbObj$connection) # forward
  }
)
