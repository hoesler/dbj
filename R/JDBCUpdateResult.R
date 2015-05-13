#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' Class JDBCUpdateResult with factory method JDBCUpdateResult.
#'
#' @export
setClass("JDBCUpdateResult",
  contains = c("JDBCResult"),
  slots = c(
    statement = "character",
    update_count = "numeric")
)

#' @param update_count the number of affected rows.
#' @param connection a \code{\linkS4class{JDBCConnection}} object.
#' @param statement statement which was used for the query which returned this result
#' @return a new JDBCUpdateResult object
#' @rdname JDBCUpdateResult-class
#' @export
JDBCUpdateResult <- function(update_count, connection, statement = "") {
  assert_that(is(update_count, "numeric"))
  assert_that(is(connection, "JDBCConnection"))
  new("JDBCUpdateResult", update_count = update_count, connection = connection, statement = statement)
}

#' Fetch records from a previously executed query
#'
#' @param res an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("fetch", signature(res = "JDBCUpdateResult", n = "numeric"),
  function(res, n, ...) {
    stop("fetch is unsupported in JDBCUpdateResult")
  }
)

#' @rdname fetch-JDBCUpdateResult-numeric-method
#' @export
setMethod("fetch", signature(res = "JDBCUpdateResult", n = "missing"),
  function(res, n, ...) {
    stop("fetch is unsupported in JDBCUpdateResult")
  }
)

#' @rdname JDBCUpdateResult-class
#' @param res an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbClearResult", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @rdname JDBCUpdateResult-class
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("dbColumnInfo is unsupported in JDBCUpdateResult")    
  },
  valueClass = "data.frame"
)

#' @rdname JDBCUpdateResult-class
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("dbGetRowCount is unsupported in JDBCUpdateResult")
  },
  valueClass = "numeric"
)

#' @rdname JDBCUpdateResult-class
#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@update_count
  },
  valueClass = "numeric"
)

#' @rdname JDBCUpdateResult-class
#' @export
setMethod("dbGetStatement", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@statement
  },
  valueClass = "character"
)

#' @rdname JDBCUpdateResult-class
#' @export
setMethod("dbHasCompleted", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    TRUE
  }
)

#' Get the names or labels for the columns of the result set.
#' 
#' @param conn an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param  name Ignored. Needed for compatiblity with generic.
#' @param  use_labels if the the method should return the labels or the names of the columns
#' @param  ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbListFields", signature(conn = "JDBCUpdateResult", name = "missing"),
  function(conn, name, use_labels = TRUE, ...) {
    stop("dbListFields is unsupported in JDBCUpdateResult")
  },
  valueClass = "character"
)

#' @rdname JDBCUpdateResult-class
#' @param object an \code{\linkS4class{JDBCUpdateResult}} object.
#' @export
setMethod("summary", "JDBCUpdateResult",
  function(object, ...) {
    info <- result_info(object)
    cat("JDBC Update Result\n")
    cat(sprintf("  Update Count: %s\n", info$update_count))
  }
)

#' Get info about the result.
#' 
#' @param dbObj an object of class \code{\linkS4class{JDBCUpdateResult}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCUpdateResult"),
  function(dbObj, ...) {
    list(
      update_count = dbObj@update_count
    )
  },
  valueClass = "list"
)
