#' @include JDBCResult.R
#' @include JavaUtils.R
NULL

#' Class JDBCUpdateResult with factory method JDBCUpdateResult.
#'
#' @keywords internal
#' @export
setClass("JDBCUpdateResult",
  contains = c("JDBCResult"),
  slots = c(
    statement = "character",
    update_count = "numeric",
    connection = "JDBCConnection")
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
  new("JDBCUpdateResult",
    update_count = update_count,
    connection = connection,
    statement = statement
  )
}

#' @describeIn JDBCUpdateResult Returns an empty data frame.
#'
#' @param res an \code{\linkS4class{JDBCUpdateResult}} object.
#' @param n optional maximum number of records to retrieve per fetch. Use \code{-1} to 
#'    retrieve all pending records; use \code{0} for to fetch the default 
#'    number of rows as defined in \code{\link{JDBC}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("fetch", signature(res = "JDBCUpdateResult", n = "numeric"),
  function(res, n, ...) {
    data.frame()
  }
)

#' @describeIn JDBCUpdateResult Returns an empty data frame.
#' @export
setMethod("fetch", signature(res = "JDBCUpdateResult", n = "missing"),
  function(res, n, ...) {
    data.frame()
  }
)

#' @describeIn JDBCUpdateResult Has no effect. Returns always \code{TRUE}.
#' @export
setMethod("dbClearResult", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCUpdateResult Unsupported
#' @export
setMethod("dbColumnInfo", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("dbColumnInfo is unsupported in JDBCUpdateResult")    
  },
  valueClass = "data.frame"
)

#' @describeIn JDBCUpdateResult Unsupported
#' @export
setMethod("dbGetRowCount", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    stop("dbGetRowCount is unsupported in JDBCUpdateResult")
  },
  valueClass = "numeric"
)

#' @describeIn JDBCUpdateResult Get info
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCUpdateResult"),
  function(dbObj, ...) {
    list(
      statement = dbGetStatement(dbObj),
      rows.affected = dbGetRowsAffected(dbObj),
      has.completed = TRUE,
      is.select = FALSE
    )
  },
  valueClass = "list"
)

#' @describeIn JDBCUpdateResult Is always \code{TRUE}.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCUpdateResult"),
  function(dbObj, ...) {
    TRUE
  },
  valueClass = "logical"
)

#' @rdname JDBCUpdateResult-class
#' @param dbObj An object of class \code{\linkS4class{JDBCUpdateResult}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbGetDriver", signature(dbObj = "JDBCUpdateResult"),
  function(dbObj, ...) {
    dbGetDriver(dbObj@connection) # forward
  }
)

#' @rdname JDBCUpdateResult-class
#' @section Methods:
#' \code{dbHasCompleted}: Check if all results have been fetched
#' @export
setMethod("dbHasCompleted", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    TRUE
  }
)

#' @rdname JDBCUpdateResult-class
#' @section Methods:
#' \code{dbGetStatement}: Returns the statement that was passed to dbSendQuery
#' @export
setMethod("dbGetStatement", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@statement
  }
)

#' @rdname JDBCUpdateResult-class
#' @section Methods:
#' \code{dbGetRowsAffected}: Returns the number of rows that were added, deleted, or updated
#' @export
setMethod("dbGetRowsAffected", signature(res = "JDBCUpdateResult"),
  function(res, ...) {
    res@update_count
  }
)
