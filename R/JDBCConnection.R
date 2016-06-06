#' @include JDBCObject.R
#' @include JDBCDriver.R
#' @include JDBCConnectionExtensions.R
#' @include JavaUtils.R
#' @include JDBCUtils.R
#' @include RUtils.R
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
      .Object@info <- connection_info(j_connection)
    }
    .Object@state$savepoints <- list()
    .Object
  }
)

connection_info <- function(j_connection) {
  j_dbmeta <- jtry(jcall(j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData"))
  
  list(
      db.version = jtry(jcall(j_dbmeta, "S", "getDatabaseProductVersion")),
      dbname = jtry(jcall(j_dbmeta, "S", "getDatabaseProductName")),
      username = jtry(jcall(j_dbmeta, "S", "getUserName")), 
      host = NULL,
      port = NULL,

      url = jtry(jcall(j_dbmeta, "S", "getURL")),
      jdbc_driver_name = jtry(jcall(j_dbmeta, "S", "getDriverName")),
      jdbc_driver_version = jtry(jcall(j_dbmeta, "S", "getDriverVersion")),

      feature.savepoints = jtry(jcall(j_dbmeta, "Z", "supportsSavepoints"))
    )
}

add_savepoint <- function(connection, savepoint_name, j_savepoint) {
  connection@state$savepoints$savepoint_name <- j_savepoint
}

remove_savepoint <- function(connection, savepoint_name) {
  savepoints <- connection@state$savepoints
  connection@state$savepoints$savepoint_name <- NULL
  savepoint <- savepoints$savepoint_name
}

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
  },
  valueClass = "JDBCConnection"
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
  },
  valueClass = "logical"
)

#' Disconnect (close) a connection
#' 
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @export
setMethod("dbDisconnect", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    if (!jtry(jcall(conn@j_connection, "Z", "isClosed"))) {
      jtry(jcall(conn@j_connection, "V", "close"))
    } else {
      warning("Connection has already been closed") # required by DBItest
    }
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Execute a SQL statement on a database connection
#'
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.-JDBCConnection-method
#' @param statement the statement to send over the connection
#' @param parameters a list of statement parameters
#' @export
setMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, parameters = list(), ...) {
    assert_that(is.list(parameters))
    statement <- as.character(statement)[1L]
    
    j_statement <- create_prepared_statement(conn, statement)
    insert_parameters(j_statement, parameters, dbGetDriver(conn)@write_conversions)
    hasResult <- execute_query(j_statement)
    
    if (hasResult) {
      j_result_set <- jtry(jcall(j_statement, "Ljava/sql/ResultSet;", "getResultSet"))
      conn@create_new_query_result(j_result_set, conn, statement)
    } else {
      update_count <- jtry(jcall(j_statement, "I", "getUpdateCount"))
      conn@create_new_update_result(update_count, conn, statement)
    }
    
  },
  valueClass = "JDBCResult"
)

#' @describeIn dbSendUpdate Send update query without parameters
#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param statement the statement to send over the connection
#' @param parameters a list of statement parameters
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "missing"),
  function(conn, statement, parameters, ...) {
    j_statement <- create_prepared_statement(conn, statement)
    on.exit(close_statement(j_statement))
    execute_update(j_statement)
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn dbSendUpdate Send update query with parameters given as a named list
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "list"),
  function(conn, statement, parameters, ...) {
    assert_that(!is.null(names(parameters)))
    dbSendUpdate(conn, statement, as.data.frame(parameters))
  },
  valueClass = "logical"
)


#' @describeIn dbSendUpdate Send batch update queries with parameters given as a data.frame
#' @param partition_size the size which will be used to partition the data into seperate commits
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "data.frame"),
  function(conn, statement, parameters, partition_size = 10000, ...) {
    assert_that(!is.null(names(parameters)))
    assert_that(!any(is.na(names(parameters))))
    assert_that(length(statement) == 1)
    assert_that(nrow(parameters) > 0)

    conversions <- dbGetDriver(conn)@write_conversions

    sapply(partition(parameters, partition_size), function(subset) {
      # Create a new statement for each batch. Reusing a single statement messes up ParameterMetaData (on H2).
      j_statement <- create_prepared_statement(conn, statement)
      tryCatch({
        batch_insert(j_statement, subset, conversions)
        execute_batch(j_statement)},
      finally = close_statement(j_statement))   
    })    

    invisible(TRUE)
  },
  valueClass = "logical"
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
  },
  valueClass = "character"
)

#' @param pattern the pattern for table names
#' @describeIn dbGetTables Get tables for the JDBCConnection
#' @export
setMethod("dbGetTables", signature(conn = "JDBCConnection"),
  function(conn, pattern = "%", ...) {
    md <- jtry(jcall(conn@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData"),
      jstop, "Failed to retrieve JDBC database metadata")
    # getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
    j_result_set <- jtry(.jcall(md, "Ljava/sql/ResultSet;", "getTables", .jnull("java/lang/String"),
                .jnull("java/lang/String"), pattern, .jnull("[Ljava/lang/String;"), check = FALSE),
      jstop, "Unable to retrieve JDBC tables list")    
    fetch_all(j_result_set, conn)
  },
  valueClass = "data.frame"
)

#' @describeIn JDBCConnection Does a table exist?
#' @param name A character string specifying a DBMS table name.
#' @export
setMethod("dbExistsTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    tables <- dbListTables(conn, name, ...)
    length(tables) > 0
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Executes the SQL \code{DROP TABLE}.
#' @export
setMethod("dbRemoveTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbSendUpdate(conn, paste("DROP TABLE", dbQuoteIdentifier(conn, name)))
  },
  valueClass = "logical"
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
  },
  valueClass = "data.frame"
)

#' @param conn A \code{\linkS4class{JDBCConnection}} object, as produced by
#'   \code{\link{dbConnect}}.
#' @param ... Other parameters passed on to methods.
#' @param name the pattern for table names
#' @param pattern the pattern for column names
#' @rdname dbGetFields
setMethod("dbGetFields", signature(conn = "JDBCConnection"),
  function(conn, name, pattern = "%", ...) {
    md <- jtry(jcall(conn@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData"),
      jstop, "Unable to retrieve JDBC database metadata")
    # getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    j_result_set <- jtry(.jcall(md, "Ljava/sql/ResultSet;", "getColumns",
        .jnull("java/lang/String"), .jnull("java/lang/String"), name, pattern, check = FALSE),
      jstop, "Unable to retrieve JDBC columns list for ", name)
    fetch_all(j_result_set, conn)
  },
  valueClass = "data.frame"
)

#' @describeIn JDBCConnection Copy data frames from database.
#' @export
setMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbGetQuery(conn, paste("SELECT * FROM", dbQuoteIdentifier(conn, name)))
  },
  valueClass = "data.frame"
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
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Get the current dialect
#' @export
setMethod("dbSQLDialect", signature(conn = "JDBCConnection"),
  function(conn) {
    conn@driver@dialect
  }
)

#' @describeIn JDBCConnection Begin a transaction
#' @param savepoint_name The name of the savepoint
#' @export
setMethod("dbBegin", signature(conn = "JDBCConnection"),
  function(conn, savepoint_name, ...) {
    jtry(jcall(conn@j_connection, "V", "setAutoCommit", FALSE))
    if (dbGetInfo(conn)$feature.savepoints) {
      j_savepoint <- jtry(jcall(conn@j_connection, "Ljava/sql/Savepoint;", "setSavepoint", savepoint_name))
      add_savepoint(conn, savepoint_name, j_savepoint)      
    } else {
      warning("Savepoints are not supported")
    }
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Commit a transaction
#' @export
setMethod("dbCommit", signature(conn = "JDBCConnection"),
  function(conn, savepoint_name = NULL, ...) {
    if (!is.null(savepoint_name)) {
      remove_savepoint(conn, savepoint_name)
    }
    jtry(jcall(conn@j_connection, "V", "commit"))
    jtry(jcall(conn@j_connection, "V", "setAutoCommit", TRUE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Rollback a transaction
#' @export
setMethod("dbRollback", signature(conn = "JDBCConnection"), 
  function(conn, savepoint_name, ...) {
    j_savepoint <- remove_savepoint(conn, savepoint_name)
    if (!is.null(j_savepoint)) {
      jtry(jcall(conn@j_connection, "V", "rollback", j_savepoint))
    } else {
      jtry(jcall(conn@j_connection, "V", "rollback"))
    }
    jtry(jcall(conn@j_connection, "V", "setAutoCommit", TRUE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Returns a list with \code{dbname}, \code{db.version}, \code{username}, \code{jdbc_driver_name}, \code{jdbc_driver_version}, \code{url}, \code{host} and \code{port}.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    dbObj@info
  },
  valueClass = "list"
)

#' @describeIn JDBCConnection Not implemented. Returns an empty list.
#' @export
setMethod("dbGetException", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    warning("Not implemented. Returns an empty list.")
    list(errNum = "", errMsg = "")
  },
  valueClass = "list"
)

#' @describeIn JDBCConnection Returns an empty \code{list} as JDBC maintains no list of active results.
#' @export
setMethod("dbListResults", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    list()
  },
  valueClass = "list"
)

#' @describeIn JDBCConnection Returns the list of SQL keywords as defined in the DatabaseMetaData Java object of the associated Java Connection object.
#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    .Deprecated()
    md <- jtry(jcall(dbObj@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData"))
    keywords <- jtry(jcall(md, "S", "getSQLKeywords"))
    unique(c(unlist(strsplit(keywords, "\\s*,\\s*")), .SQL92Keywords)) # TODO Java API refers to SQL:2003 keywords
  },
  valueClass = "character"
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
#' @param timeout The time in seconds to wait for the database operation used to validate the connection to complete. If the timeout period expires before the operation completes, this method returns false. A value of 0 indicates a timeout is not applied to the database operation.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCConnection"),
  function(dbObj, timeout = 0, ...) {
    !is.jnull(dbObj@j_connection) && jtry(jcall(dbObj@j_connection, "Z", "isValid", as.integer(timeout)))
  },
  valueClass = "logical"
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
