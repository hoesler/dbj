#' @include JDBCConnection.R
NULL

#' JDBC transaction management.
#' 
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param savepoint_name Supply a name to use a named savepoint. This allows you to
#'   nest multiple transaction
#' @return A boolean, indicating success or failure.
#' @name jdbc-transaction
NULL

add_savepoint <- function(connection, savepoint_name, j_savepoint) {
  connection@state$savepoints$savepoint_name <- j_savepoint
}

remove_savepoint <- function(connection, savepoint_name) {
  savepoints <- connection@state$savepoints
  connection@state$savepoints$savepoint_name <- NULL
  savepoint <- savepoints$savepoint_name
}

#' @export
#' @rdname jdbc-transaction
setMethod("dbBegin", signature(conn = "JDBCConnection"),
  function(conn, savepoint_name = NULL) {
    jdbc_connection_autocommit(conn@j_connection, FALSE)
    if (dbGetInfo(conn)$feature.savepoints) {
      j_savepoint <- jdbc_connection_set_savepoint(conn@j_connection, savepoint_name)
      add_savepoint(conn, savepoint_name, j_savepoint)      
    } else {
      if (!is.null(savepoint_name)) {
        warning("Savepoints are not supported")
      }
    }
    TRUE
  }
)

#' @export
#' @rdname jdbc-transaction
setMethod("dbCommit", signature(conn = "JDBCConnection"),
  function(conn, savepoint_name = NULL) {
    if (!is.null(savepoint_name)) {
      remove_savepoint(conn, savepoint_name)
    }
    jdbc_connection_commit(conn@j_connection)
    jdbc_connection_autocommit(conn@j_connection, TRUE)
    TRUE
  }
)

#' @export
#' @rdname jdbc-transaction
setMethod("dbRollback", signature(conn = "JDBCConnection"), 
  function(conn, savepoint_name = NULL) {
    j_savepoint <- if (is.null(savepoint_name)) NULL else remove_savepoint(conn, savepoint_name)
    jdbc_connection_rollback(conn@j_connection, j_savepoint)
    jdbc_connection_autocommit(conn@j_connection, TRUE)
    TRUE
  }
)
