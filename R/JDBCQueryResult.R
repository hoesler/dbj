#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' JDBCQueryResult class.
#'
#' @keywords internal
#' @export
#' @include JDBCObject.R
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

#' @describeIn JDBCQueryResult fetch n results
#' @param res an \code{\linkS4class{JDBCQueryResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param fetch_size a hint to the number of rows that should be fetched from the database in a single block.
#'    See \url{http://docs.oracle.com/javase/7/docs/api/java/sql/Statement.html#setFetchSize(int)}.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "numeric"),
  function(res, n, fetch_size = 0, ...) {
    assert_that(is.numeric(fetch_size))    

    cols <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    if (cols < 1L) {
      return(NULL)
    }
       
    column_info <- dbColumnInfo(res, c("label", "nullable", "field.type"))

    infinite_pull <- (n == -1)
    stride <- if (infinite_pull) {
      32768L  #start fairly small to support tiny queries and increase later
      # TODO does the increaing stride srategy really has any performnace / memory benefit?
    } else {
      n
    }

    chunks <- list()
    repeat {    
      fetched <- fetch_resultpull(res@j_result_pull, stride, column_info, dbGetDriver(res)@read_conversions, fetch_size) 
      chunks <- c(chunks, list(fetched))

      if (!infinite_pull || nrow(fetched) < stride) {
        break
      }

      stride <- 524288L # 512k
    }

    do.call(rbind, chunks)
  }
)

#' @describeIn JDBCQueryResult fetch all results
#' @export
setMethod("fetch", signature(res = "JDBCQueryResult", n = "missing"),
  function(res, n, ...) {
    fetch(res, -1)
  }
)

#' @describeIn JDBCQueryResult Clear results
#' @export
setMethod("dbClearResult", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    closed <- jtry(.jcall(res@j_result_set, "Z", "isClosed", check = FALSE))
    if (!closed) {
      j_statement <- jtry(.jcall(res@j_result_set, "Ljava/sql/Statement;", "getStatement", check = FALSE))
      if (!is.jnull(j_statement)) {
        close_statement(j_statement)
      } else {
        close_result_set(res@j_result_set)
      }
    }
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Get info about the result set data types.
#' 
#' @param res an \code{\linkS4class{JDBCQueryResult}} object.
#' @param what a character vector indicating which info to return.
#'   Expected is a subset of \code{c("name", "field.type", "data.type", "label", "nullable")}.
#' @param ... Ignored. Needed for compatiblity with generic.
#' @return A data.frame with one row per output field in \code{res}.
#'   Includes \code{name}, \code{field.type} (the SQL type)
#'   and \code{data.type} (the R data type) columns for the default what.
#'   Additionally, \code{label} (The field label)
#'   and \code{nullable} (0 = disallows NULL, 1 = allows NULL, 2 = unknown) can be fetched.
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCQueryResult"),
  function(res, what = c("name", "field.type", "data.type"), ...) {
    assert_that(is.character(what))

    if (length(what) == 0) {
      return(data.frame())
    }
    
    column_count <- jtry(.jcall(res@j_result_set_meta, "I", "getColumnCount", check = FALSE))
    
    column_info <- list()

    if ("name" %in% what) { # TODO: return lables as names?
      column_info <- c(column_info, list(name = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "S", "getColumnName", i, check = FALSE))
      }, "")))
    }

    if ("field.type" %in% what) {
      column_info <- c(column_info, list(field.type = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "S", "getColumnTypeName", i, check = FALSE))
      }, "")))
    }

    if ("data.type" %in% what) {
      read_conversions <- dbGetDriver(res)@read_conversions
      column_info <- c(column_info, list(data.type = vapply(seq(column_count), function(i) {
        field.type <- ""
        if ("field.type" %in% names(column_info)) {
          field.type <- column_info$field.type[i]
        } else {
          field.type <- jtry(.jcall(res@j_result_set_meta, "S", "getColumnTypeName", i, check = FALSE))
        }
        read_conversions[sapply(read_conversions, function (x) { x$condition(list("field.type" = field.type)) })][[1]]$r_class
      }, "")))
    }

    if ("label" %in% what) {
      column_info <- c(column_info, list(label = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "S", "getColumnLabel", i, check = FALSE))
      }, "")))
    }
    
    if ("nullable" %in% what) {
      column_info <- c(column_info, list(nullable = vapply(seq(column_count), function(i) {
        jtry(.jcall(res@j_result_set_meta, "I", "isNullable", i, check = FALSE)) # 0 = disallows NULL, 1 = allows NULL, 2 = unknown
      }, as.integer(0))))
    }

    as.data.frame(column_info, row.names = seq(column_count), stringsAsFactors = FALSE)   
  },
  valueClass = "data.frame"
)

#' @describeIn JDBCQueryResult Count roes in result set
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    # This method returns 0 before the first and after the last row.
    jtry(.jcall(res@j_result_set, "I", "getRow", check = FALSE))
  },
  valueClass = "numeric"
)

#' @describeIn JDBCQueryResult Count rows affected by the last update query
#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCQueryResult"),
  function(res, ...) {
    .NotYetImplemented()
  },
  valueClass = "numeric"
)

#' @describeIn JDBCQueryResult Check if all results have been fetched
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

#' @describeIn JDBCQueryResult Get info
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
    statement = result_set@statement,
    col.count = jtry(.jcall(result_set@j_result_set_meta, "I", "getColumnCount", check = FALSE)),
    row.count = dbGetRowCount(result_set),
    has.completed = dbHasCompleted(result_set),
    is.select = TRUE,
    # TODO: total number of records to be fetched
    rows.affected = NA
  )
}

#' @describeIn JDBCQueryResult Is the result valid
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCQueryResult"),
  function(dbObj, ...) {
    closed <- jtry(.jcall(dbObj@j_result_set, "Z", "isClosed", check = FALSE))
    return(!closed)
  },
  valueClass = "logical"
)

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