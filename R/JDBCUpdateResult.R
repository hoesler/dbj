#' @include JDBCObject.R
#' @include JavaUtils.R
NULL

#' Class JDBCUpdateResult with factory method JDBCUpdateResult.
#'
#' @name JDBCUpdateResult-class
#' @docType class
#' @rdname JDBCUpdateResult-class
#' @export
setClass("JDBCUpdateResult",
  contains = c("DBIResult", "JDBCObject"),
  slots = c(
    update_count = "numeric")
)

#' @param j_result_set a \code{\linkS4class{jobjRef}} object which holds a reference to a \code{java/sql/ResultSet} Java object.
#' @param statement the stament that was used to create the j_result_set
#' @return a new JDBCUpdateResult object
#' @rdname JDBCDriver-class
#' @export
JDBCUpdateResult <- function(update_count, statement = "") {
  assert_that(update_count, is_a("numeric"))
  new("JDBCUpdateResult", update_count = update_count)
}

#' @rdname fetch-JDBCUpdateResult-numeric-method
#' @export
setMethod("fetch", signature(res = "JDBCUpdateResult", n = "missing"),
  function(res, n, ...) {
    stop("This is an update result")
  }
)

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
    stop("This is an update result")
  }
)

#' Clear a result set.
#' 
#' @param res an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbClearResult", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("This is an update result")
  },
  valueClass = "logical"
)

#' Get info about the result set data types.
#' 
#' @param res an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("This is an update result")    
  },
  valueClass = "data.frame"
)

#' @export
setMethod("dbGetException", signature(conn = "JDBCUpdateResult"),
  function(conn, ...) {
    .NotYetImplemented()
  }
)

#' Get number of rows fetched so far
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("This is an update result")
  },
  valueClass = "numeric"
)

#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@update_count
  },
  valueClass = "numeric"
)

#' @export
setMethod("dbGetStatement", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@statement
  },
  valueClass = "character"
)

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
    stop("This is an update result")
  },
  valueClass = "character"
)

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

#' @export
setMethod("coerce", signature(from = "JDBCConnection", to = "JDBCUpdateResult"),
  function(from, to) {
    .NotYetImplemented() # TODO: I have no idea what the api wants us to do here
  }
)