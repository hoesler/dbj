#' @include JDBCObject.R
#' @include JDBCDriver.R
#' @include JDBCConnectionExtensions.R
#' @include JavaUtils.R
#' @include SQLUtils.R
#' @include JDBCUtils.R
#' @include RUtils.R
NULL

#' Class JDBCConnection
#' 
#' @param conn a \code{\linkS4class{JDBCConnection}} object
#' @param dbObj a \code{\linkS4class{JDBCConnection}} object
#' @param ... Arguments passed on to other methods.
#' 
#' @export
setClass("JDBCConnection", contains = c("DBIConnection", "JDBCObject"),
  slots = c(
    j_connection = "jobjRef",
    identifier_quote_string = "character",
    driver = "JDBCDriver"),
  validity = function(object) {
    if (is.jnull(object@j_connection)) return("j_connection is null")
    if (is.null(object@driver)) return("driver is null")
    TRUE
  }
)

#' Create a new \code{\linkS4class{JDBCConnection}} object
#' 
#' @param j_connection a jobjRef object with a java.sql.Connection reference
#' @param driver the full qualified class name of the JDBC Driver to use.
#' @return a \code{\linkS4class{JDBCConnection}} object
#' @keywords internal
JDBCConnection <- function(j_connection, driver) {
  assert_that(j_connection %instanceof% "java.sql.Connection")
  new("JDBCConnection",
    j_connection = j_connection,
    identifier_quote_string = connection_info(j_connection)$identifier_quote_string,
    driver = driver)
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
    info <- connection_info(drv@j_connection)
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
    if (!jtry(.jcall(conn@j_connection, "Z", "isClosed", check = FALSE))) {
      jtry(.jcall(conn@j_connection, "V", "close", check = FALSE))
    } else {
      stop("Connection has already been closed") # required by DBItest
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
      j_result_set <- jtry(.jcall(j_statement, "Ljava/sql/ResultSet;", "getResultSet", check = FALSE))
      JDBCQueryResult(j_result_set, conn, statement)
    } else {
      update_count <- jtry(.jcall(j_statement, "I", "getUpdateCount", check = FALSE))
      JDBCUpdateResult(update_count, conn, statement)
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

fetch_all <- function(j_result_set, connection, close = TRUE) {  
  res <- JDBCQueryResult(j_result_set, connection)
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
    md <- jtry(.jcall(conn@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE),
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
    md <- jtry(.jcall(conn@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE),
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
#' @export
setMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame"),
  function(conn, name, value, create = TRUE, append = FALSE, truncate = FALSE, ...) { 
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
    
    commit_automatically <- jtry(.jcall(conn@j_connection, "Z", "getAutoCommit", check = FALSE))
    if (commit_automatically) {
      jtry(.jcall(conn@j_connection, "V", "setAutoCommit", FALSE, check = FALSE)) # TODO: switch to dbBegin here?
      on.exit(jtry(.jcall(conn@j_connection, "V", "setAutoCommit", commit_automatically, check = FALSE)))
    }
    
    escaped_table_name <- dbQuoteIdentifier(conn, name)

    if (!table_exists && create) {
      data_types <- sapply(value, dbDataType, dbObj = conn)
      field_definitions <- paste(dbQuoteIdentifier(conn, names(value)), data_types, collapse = ', ')
      
      statement <- sprintf("CREATE TABLE %s (%s)", escaped_table_name, field_definitions)
      table_was_created <- dbSendUpdate(conn, statement)
      if (!table_was_created) {
        stop("Table could not be created")
      }
    }

    if (table_exists && truncate) {
      truncated <- dbTruncateTable(conn, name, use_delete = TRUE)
      if (!truncated) {
        stop(sprintf("Table %s could not be truncated", name))
      }
    }
    
    if (nrow(value) > 0) {
      statement <- sprintf("INSERT INTO %s(%s) VALUES(%s)",
        escaped_table_name,
        paste(dbQuoteIdentifier(conn, names(value)), collapse = ', '),
        paste(rep("?", length(value)), collapse = ', '))
      
      dbSendUpdate(conn, statement, parameters = value)
    }
    
    if (commit_automatically) {
      dbCommit(conn) 
    }

    invisible(TRUE)           
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Begin a transaction
#' @export
setMethod("dbBegin", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "setAutoCommit", FALSE, check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Commit a transaction
#' @export
setMethod("dbCommit", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "commit", check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Rollback a transaction
#' @export
setMethod("dbRollback", signature(conn = "JDBCConnection"), 
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "rollback", check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @describeIn JDBCConnection Returns a list with \code{database_product_name}, \code{database_product_version}, \code{driver_name}, \code{driver_version}, \code{url}, \code{username}, \code{identifier_quote_string}, \code{db.version}, \code{dbname}, \code{host} and \code{port}.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    connection_info(dbObj@j_connection)
  },
  valueClass = "list"
)

connection_info <- function(j_connection) {
  md <- jtry(.jcall(j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE))
  list(
    database_product_name = jtry(.jcall(md, "S", "getDatabaseProductName", check = FALSE)),
    database_product_version = jtry(.jcall(md, "S", "getDatabaseProductVersion", check = FALSE)),
    driver_name = jtry(.jcall(md, "S", "getDriverName", check = FALSE)),
    driver_version = jtry(.jcall(md, "S", "getDriverVersion", check = FALSE)),
    url = jtry(.jcall(md, "S", "getURL", check = FALSE)),
    username = jtry(.jcall(md, "S", "getUserName", check = FALSE)),
    identifier_quote_string = jtry(.jcall(md, "S", "getIdentifierQuoteString", check = FALSE)),
    # TODO: get data for the folowing keys as required by DBI
    db.version = "",
    dbname = "",
    host = "",
    port = ""
  )
}

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
    md <- jtry(.jcall(dbObj@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE))
    keywords <- jtry(.jcall(md, "S", "getSQLKeywords", check = FALSE))
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
    if (use_delete) {
      dbSendUpdate(conn, sprintf("DELETE FROM %s", dbQuoteIdentifier(conn, name)))
    } else {
      dbSendUpdate(conn, sprintf("TRUNCATE TABLE %s", dbQuoteIdentifier(conn, name)))      
    }
  }
)

#' dbIsValid
#' @param dbObj A subclass of DBIConnection, representing an active connection to an DBMS.
#' @param timeout The time in seconds to wait for the database operation used to validate the connection to complete. If the timeout period expires before the operation completes, this method returns false. A value of 0 indicates a timeout is not applied to the database operation.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCConnection"),
  function(dbObj, timeout = 0, ...) {
    !is.jnull(dbObj@j_connection) && jtry(.jcall(dbObj@j_connection, "Z", "isValid", as.integer(timeout), check = FALSE))
  },
  valueClass = "logical"
)

#' Quote identifier
#' @param conn A subclass of DBIConnection, representing an active connection to an DBMS.
#' @param x A character vector to label as being escaped SQL.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbQuoteIdentifier", signature(conn = "JDBCConnection", x = "character"),
  function(conn, x, ...) {
    qs <- conn@identifier_quote_string
    x <- gsub(qs, sprintf("%s%s", qs, qs), x, fixed = TRUE)
    SQL(paste(qs, x, qs, sep = ""))
  }
)

#' Quote identifier
#' @param conn A subclass of DBIConnection, representing an active connection to an DBMS.
#' @param x A character vector to label as being escaped SQL.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbQuoteIdentifier", signature(conn = "JDBCConnection", x = "SQL"),
  function(conn, x, ...) {
    x
  }
)
