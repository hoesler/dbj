#' @include JDBCObject.R
#' @include JDBCDriver.R
#' @include JDBCConnection_generics.R
#' @include java_utils.R
#' @include java_jdbc_utils.R
#' @include r_utils.R
NULL

#' Class JDBCConnection
#' 
#' @param conn,con,dbObj A \code{\linkS4class{JDBCConnection}} object.
#' @param ... Arguments passed on to other methods.
#' 
#' @slot state An environment for mutable states
#' @slot j_connection A \code{jobjRef} holding a java.sql.Connection.
#' @slot info The connection info list.
#' @slot driver A \code{\linkS4class{JDBCDriver}} object.
#' @slot create_new_query_result The factory function for \code{\linkS4class{JDBCQueryResult}} objects.
#' @slot create_new_update_result The factory function for \code{\linkS4class{JDBCUpdateResult}} objects.
#' 
#' @export
JDBCConnection <- setClass("JDBCConnection", contains = c("DBIConnection", "JDBCObject"),
  slots = c(
    state = "environment",
    j_connection = "jobjRef",
    info = "list",
    driver = "JDBCDriver",
    create_new_query_result = "function",
    create_new_update_result = "function"),
  validity = function(object) {
    if (is.jnull(object@j_connection)) return("j_connection is null")
    if (is.null(object@driver)) return("driver is null")
    TRUE
  }
)

setMethod("initialize", "JDBCConnection", function(.Object, j_connection, ...) {
    .Object <- callNextMethod()
    if (!missing(j_connection)) {
      .Object@info <- jdbc_connection_info(j_connection)
    }
    .Object@state$savepoints <- list()
    .Object
  }
)

#' Create a JDBC connection.
#' 
#' Create a connection to the same URL and with the same user as this connection uses.
#' @param drv a \code{\linkS4class{JDBCConnection}} object
#' @param password the password for the connection
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbConnect", signature(drv = "JDBCConnection"),
  function(drv, password = "", ...) {
    info <- dbGetInfo(drv)
    dbConnect(dbGetDriver(drv), info$url, info$user_name, password)
  }
)

#' Marked as deprecated in DBI
#'
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param name the name of the procedure
#' @param parameters a list of procedure arguments.
#' @export
setMethod("dbCallProc", signature(conn = "JDBCConnection"),
  function(conn, name, parameters = list(), ...) {
    .Deprecated("dbSendUpdate")
    assert_that(is(name, "character"))
    assert_that(length(name) == 1)
    assert_that(is(parameters, "list"))

    j_prepared_statement <- prepare_call(conn, sprintf("{call %s(%s)}",
      dbQuoteIdentifier(conn, name),
      paste0(rep('?', length(parameters)), collapse = ", ")
    ))
    insert_parameters(j_prepared_statement, parameters, dbGetDriver(conn)@write_conversions)
    execute_update(j_prepared_statement)
    invisible(TRUE)
  }
)

#' Disconnect (close) a connection
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @export
setMethod("dbDisconnect", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    if (!jdbc_connection_is_closed(conn@j_connection)) {
      jdbc_close_connection(conn@j_connection)
    } else {
      warning("Connection has already been closed") # required by DBItest
    }
    invisible(TRUE)
  }
)

#' Execute a SQL statement on a database connection
#'
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param statement A SQL statement to send over the connection. Use \code{?} for input parameters.
#' @param parameters A list of statement parameters, which will be inserted in order.
#' @return A \code{\linkS4class{JDBCResult}} object
#' @export
setMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, parameters = list()) {
    assert_that(is.list(parameters))
    statement <- as.character(statement)[1L]
    
    j_statement <- create_prepared_statement(conn, statement)
    insert_parameters(j_statement, parameters, dbGetDriver(conn)@write_conversions)
    hasResult <- execute_query(j_statement)
    
    if (hasResult) {
      j_result_set <- jdbc_get_result_set(j_statement)
      conn@create_new_query_result(j_result_set, conn, statement)
    } else {
      update_count <- jdbc_get_update_count(j_statement)
      conn@create_new_update_result(update_count, conn, statement)
    }
    
  }
)

fetch_all <- function(j_result_set, connection, statement = "", close = TRUE) {  
  res <- connection@create_new_query_result(j_result_set, connection, statement)
  if (close) {
    on.exit(dbClearResult(res))
  }
  fetch(res, -1)
}

#' List available JDBC tables.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param pattern the pattern passed to the java method ResultSet.getTables()
#' @export
setMethod("dbListTables", signature(conn = "JDBCConnection"),
  function(conn, pattern = "%", ...) {
    tables <- dbGetTables(conn, pattern, ...)
    as.character(tables$TABLE_NAME)
  }
)

#' @param pattern the pattern for table names
#' @describeIn dbGetTables Get tables for the JDBCConnection
#' @export
setMethod("dbGetTables", signature(conn = "JDBCConnection"),
  function(conn, pattern = "%", ...) {
    j_database_meta <- jdbc_get_database_meta(conn@j_connection)
    j_result_set <- jdbc_dbmeta_get_tables(j_database_meta, pattern) 
    fetch_all(j_result_set, conn)
  }
)

#' @describeIn JDBCConnection Does a table exist?
#' @param name A character string specifying a DBMS table name.
#' @export
setMethod("dbExistsTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    tables <- dbListTables(conn, name, ...)
    length(tables) > 0
  }
)

#' @describeIn JDBCConnection Executes \code{sql_remove_table} from \code{dbSQLDialect(conn)}.
#' @export
setMethod("dbRemoveTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    sql <- with(dbSQLDialect(conn), sql_remove_table(conn, name, ...))
    dbSendUpdate(conn, sql)
  }
)

#' List field names in specified table.
#'
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param name A character string specifying a DBMS table name.
#' @param pattern the pattern for the columns to list
#' @export
setMethod("dbListFields", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, pattern = "%", ...) {
    dbGetFields(conn, name, pattern, ...)$COLUMN_NAME
  }
)

#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param name the pattern for table names
#' @param pattern the pattern for column names
#' @rdname dbGetFields
setMethod("dbGetFields", signature(conn = "JDBCConnection"),
  function(conn, name, pattern = "%", ...) {
    j_database_meta <- jdbc_get_database_meta(conn@j_connection)
    j_result_set <- jdbc_dbmeta_get_columns(j_database_meta, name, pattern)
    fetch_all(j_result_set, conn)
  }
)

#' @describeIn JDBCConnection Copy data frames from database.
#' @export
setMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbGetQuery(conn, paste("SELECT * FROM", dbQuoteIdentifier(conn, name))) # TODO: move sql to dialect
  }
)

#' Write a local data frame or file to the database.
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param name character vector of length 1 giving name of table to write to
#' @param value the date frame to write to the table
#' @param create a logical specifying whether to create a new table if it does not exist. Its default is TRUE.
#' @param append a logical specifying whether to append to an existing table. Its default is FALSE
#' @param truncate a logical specifying whether to truncate an existing table before appending. Its default is FALSE
#' @param temporary \code{TRUE} if the table should be temporary
#' @inheritParams DBI::rownames
#' @export
setMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame"),
  function(conn, name, value, create = TRUE, append = FALSE, truncate = FALSE, temporary = FALSE, row.names = NA, ...) { 
    assert_that(ncol(value) > 0)
    assert_that(!is.null(names(value)))
    assert_that(!any(is.na(names(value))))
    assert_that(is(create, "logical"))
    assert_that(is(append, "logical"))   
    
    table_exists <- dbExistsTable(conn, name)

    if (table_exists && !append) {
      stop("Table `", name, "' exists and append is FALSE")
    }

    if (!table_exists && !create) {
      stop("Table `", name, "' does not exist and create is FALSE")
    }      
    
    dbBegin(conn, "dbWriteTable")
    on.exit(dbRollback(conn, "dbWriteTable"))
    
    if (!table_exists && create) {
      sql <- with(dbSQLDialect(conn), sql_create_table(conn, name, value, temporary = temporary, row.names = row.names))
      table_was_created <- dbSendUpdate(conn, sql)
      if (!table_was_created) {
        stop("Table could not be created")
      }
    }

    if (table_exists && truncate) {
      sql <- with(dbSQLDialect(conn), sql_clear_table(conn, name, use_delete = TRUE))
      truncated <- dbSendUpdate(conn, sql)
      if (!truncated) {
        stop(sprintf("Table %s could not be truncated", name))
      }
    }
    
    if (nrow(value) > 0) {
      value <- sqlRownamesToColumn(value, row.names = row.names)
      sql <- with(dbSQLDialect(conn), sql_append_table(conn, name, value, row.names = row.names, temporary = temporary))
      appended <- dbSendUpdate(conn, sql, value)
      if (!appended) {
        stop("Data could not be appended")
      }
    }

    on.exit(NULL)
    dbCommit(conn, "dbWriteTable")

    invisible(TRUE)           
  }
)

#' @describeIn JDBCConnection Get the current dialect
#' @export
setMethod("dbSQLDialect", signature(conn = "JDBCConnection"),
  function(conn) {
    conn@driver@dialect
  }
)

#' @describeIn JDBCConnection Returns a list with \code{dbname}, \code{db.version}, \code{username},
#'             \code{jdbc_driver_name}, \code{jdbc_driver_version}, \code{url}, \code{host} and \code{port}.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    dbObj@info
  }
)

#' @describeIn JDBCConnection Not implemented. Returns an empty list.
#' @export
setMethod("dbGetException", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    warning("Not implemented. Returns an empty list.")
    list(errNum = "", errMsg = "")
  }
)

#' @describeIn JDBCConnection Returns an empty \code{list} as JDBC maintains no list of active results.
#' @export
setMethod("dbListResults", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    list()
  }
)

#' @describeIn JDBCConnection Returns the driver for this connection.
#' @export
setMethod("dbGetDriver", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    dbObj@driver
  }
)

#' Truncate a table.
#' 
#' @inheritParams dbTruncateTable
#' @param use_delete Send a DELETE FROM query instead of TRUNCATE TABLE. Default is FALSE.
setMethod("dbTruncateTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, use_delete = FALSE, ...) {
    sql <- with(dbSQLDialect(conn), sql_clear_table(conn, name, use_delete))
    dbSendUpdate(conn, sql)      
  }
)

#' dbIsValid
#' @param dbObj A subclass of DBIConnection, representing an active connection to an DBMS.
#' @param timeout The time in seconds to wait for the database operation used to validate the connection to complete.
#'                If the timeout period expires before the operation completes, this method returns false.
#'                A value of 0 indicates a timeout is not applied to the database operation.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCConnection"),
  function(dbObj, timeout = 0, ...) {
    !is.jnull(dbObj@j_connection) && jdbc_connection_is_valid(dbObj@j_connection, timeout)
  }
)

#' Quote an identifier using the \code{sql_quote_identifier} function defined in the Drivers \code{sql_dialect} evironment.  
#' @inheritParams dbDisconnect,JDBCConnection-method
#' @param x A character vector to label as being escaped SQL.
#' @export
setMethod("dbQuoteIdentifier", signature(conn = "JDBCConnection", x = "character"),
  function(conn, x, ...) {
    with(dbSQLDialect(conn), sql_quote_identifier(conn, x, ...))
  }
)

#' Quote a string using the \code{sql_quote_string} function defined in the Drivers \code{sql_dialect} evironment.  
#' @inheritParams dbDisconnect,JDBCConnection-method
#' @param x A character vector to label as being escaped SQL.
#' @export
setMethod("dbQuoteString", signature(conn = "JDBCConnection", x = "character"),
  function(conn, x, ...) {
    with(dbSQLDialect(conn), sql_quote_string(conn, x, ...))
  }
)

#' @describeIn JDBCConnection Prints a short info about the connection
#' @param object An object of class \code{\linkS4class{JDBCConnection}}
#' @export
setMethod("show", "JDBCConnection", function(object) {
  cat("<JDBCConnection>\n")
  if (dbIsValid(object)) {
    cat("  URL: ", object@info$url, "\n", sep = "")
  } else {
    cat("  DISCONNECTED\n")
  }
})

#' @describeIn JDBCConnection Forwards to sql_create_table in dbSQLDialect
#' @param table A table name
#' @param fields Either a character vector or a data frame
#' @param row.names Either TRUE, FALSE, NA or a string.
#' @param temporary If TRUE, will generate a temporary table statement.
#' @export
setMethod("sqlCreateTable", "JDBCConnection",
  function(con, table, fields, row.names = NA, temporary = FALSE, ...) {
    with(dbSQLDialect(con), sql_create_table(con, table, fields, row.names = row.names, temporary = temporary, ...))
  }
)

#' @describeIn JDBCConnection Forwards to sql_create_table in dbSQLDialect
#' @param values A data frame
#' @export
setMethod("sqlAppendTable", "JDBCConnection",
  function(con, table, values, row.names = NA, ...) {
    with(dbSQLDialect(con), sql_append_table(con, table, values, row.names = row.names, ...))
  }
)
