#' @include JDBCObject.R
#' @include JDBCConnectionExtensions.R
#' @include JavaUtils.R
#' @include SQLUtils.R
#' @include JDBCUtils.R
NULL

#' Class JDBCConnection
#' 
#' @export
setClass("JDBCConnection", contains = c("DBIConnection", "JDBCObject"),
  slots = c(
    j_connection = "jobjRef",
    quote_string = "character"),
  validity = function(object) {
    if (is.jnull(object@j_connection)) return("j_connection is null")
    TRUE
  }
)

#' Create a new \code{\linkS4class{JDBCConnection}} object
#' @param j_connection a jobjRef object with a java.sql.Connection reference
#' @return a \code{\linkS4class{JDBCConnection}} object
#' @rdname JDBCConnection-class
JDBCConnection <- function(j_connection) {
  assert_that(j_connection %instanceof% "java.sql.Connection")
  new("JDBCConnection",
    j_connection = j_connection,
    quote_string = connection_info(j_connection)$quote_string)
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
    j_connection <- jtry(.jcall("java/sql/DriverManager","Ljava/sql/Connection;","getConnection",
      info$url, info$user_name, password, check = FALSE))
    JDBCConnection(j_connection)
  },
  valueClass = "JDBCConnection"
)

#' Calls a stored procedure.
#'
#' @param conn a \code{\linkS4class{JDBCConnection}} object
#' @param name the name of the procedure
#' @param parameters a list of procedure arguments.
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbCallProc", signature(conn = "JDBCConnection"),
  function(conn, name, parameters = list(), ...) {
    expect_that(name, is_a("character"))
    expect_that(length(name), equals(1))
    expect_that(parameters, is_a("list"))

    j_prepared_statement <- prepare_call(conn, sprintf("{call %s(%s)}",
      sql_escape_identifier(name),
      paste0(rep('?', length(parameters)), collapse = ", ")
    ))
    insert_parameters(j_prepared_statement, parameters)
    execute_update(j_prepared_statement)
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Disconnect an JDBC connection.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbDisconnect", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "close", check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Execute a SQL statement on a database connection
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send over the connection
#' @param parameters a list of statment parameters
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, parameters = list(), ...) {
    statement <- as.character(statement)[1L]
    
    j_statement <- create_prepared_statement(conn, statement)
    insert_parameters(j_statement, parameters)
    j_result_set <- execute_query(j_statement)
    
    JDBCResult(j_result_set, statement)
  },
  valueClass = "JDBCResult"
)

setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "missing"),
  function(conn, statement, parameters, ...) {
    j_statement <- create_prepared_statement(conn, statement)
    on.exit(close_statement(j_statement))
    execute_update(j_statement)
    invisible(TRUE)
  },
  valueClass = "logical"
)

setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "list"),
  function(conn, statement, parameters, ...) {
    expect_that(names(parameters), not(is_null()), "parameters must be a named list")
    dbSendUpdate(conn, statement, as.data.frame(parameters))
  },
  valueClass = "logical"
)

setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "data.frame"),
  function(conn, statement, parameters, ...) {
    expect_that(names(parameters), not(is_null()))
    expect_that(!any(is.na(names(parameters))), is_true())
    expect_that(length(statement), equals(1))
    expect_that(nrow(parameters), is_more_than(0))

    j_statement <- create_prepared_statement(conn, statement)
    on.exit(close_statement(j_statement))

    apply(parameters, 1, function(row) {
      insert_parameters(j_statement, as.list(row))
      add_batch(j_statement)
    })

    updates <- execute_batch(j_statement)
    invisible(as.logical(updates))
  },
  valueClass = "logical"
)

#' Execute a SQL statement on a database connection to fetch data from the database
#'
#' @param conn an object of class \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, ...) {
    result <- dbSendQuery(conn, statement, ...)
    ## Teradata needs this - closing the statement also closes the result set according to Java docs
    on.exit(dbClearResult(result))
    fetch(result, -1)
  },
  valueClass = "data.frame"
)

fetch_all <- function(j_result_set, close = TRUE) {  
  res <- JDBCResult(j_result_set)
  if (close) {
    on.exit(dbClearResult(res))
  }
  fetch(res, -1)
}

#' List available JDBC tables.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
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
#' @rdname dbGetTables
setMethod("dbGetTables", signature(conn = "JDBCConnection"),
  function(conn, pattern = "%", ...) {
    md <- jtry(.jcall(conn@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE),
      jstop, "Failed to retrieve JDBC database metadata")
    # getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
    j_result_set <- jtry(.jcall(md, "Ljava/sql/ResultSet;", "getTables", .jnull("java/lang/String"),
                .jnull("java/lang/String"), pattern, .jnull("[Ljava/lang/String;"), check = FALSE),
      jstop, "Unable to retrieve JDBC tables list")    
    fetch_all(j_result_set)
  },
  valueClass = "data.frame"
)

#' Does the table exist?
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of table
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbExistsTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    tables <- dbListTables(conn, name, ...)
    length(tables) > 0
  },
  valueClass = "logical"
)

#' Remove a table from the database.
#' 
#' Executes the SQL \code{DROP TABLE}.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of table to remove
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbRemoveTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbSendUpdate(conn, paste("DROP TABLE", name)) == 0
  },
  valueClass = "logical"
)

#' List field names in specified table.
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of the table
#' @param pattern the pattern for the columns to list
#' @param ... Ignored. Included for compatibility with generic
#' @export
setMethod("dbListFields", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, pattern = "%", ...) {
    dbGetFields(conn, name, pattern, ...)$COLUMN_NAME
  },
  valueClass = "data.frame"
)

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
    fetch_all(j_result_set)
  },
  valueClass = "data.frame"
)

#' Convenience functions for importing/exporting DBMS tables
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbGetQuery(conn, paste("SELECT * FROM", sql_escape(name,TRUE, quote_string(conn))))
  },
  valueClass = "data.frame"
)

#' Write a local data frame or file to the database.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of table to write to
#' @param value the date frame to write to the table
#' @param overwrite a logical value indicating if the table should be overwritten if it exists
#' @param append a logical value indicating if the data should get appended to an existing table
#' @export
setMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame"),
  function(conn, name, value, overwrite = TRUE, append = FALSE, ...) {
    expect_that(overwrite, is_a("logical"))
    expect_that(append, is_a("logical"))    
    expect_that(!all(overwrite, append), is_true(), "Cannot overwrite and append simultaneously")
    expect_that(ncol(value), is_more_than(0), "value must have at least one column")
    expect_that(names(value), not(is_null()))
    expect_that(any(is.na(names(value))), is_false())

    table_exists <- dbExistsTable(conn, name)
    if (append && !table_exists) {
      stop("Cannot append to a non-existing table `", name, "'")
    }
    if (table_exists && !(overwrite || append)) {
      stop("Table `", name, "' exists and neither overwrite nor append is TRUE")
    }

    if (overwrite && table_exists) {
      removed <- dbRemoveTable(conn, name)
      if (!removed) {
        stop("Failed to remove table `", name, "'")
      }
    }    
    
    commit_automatically <- jtry(.jcall(conn@j_connection, "Z", "getAutoCommit", check = FALSE))
    if (commit_automatically) {
      jtry(.jcall(conn@j_connection, "V", "setAutoCommit", FALSE, check = FALSE))
      on.exit(jtry(.jcall(conn@j_connection, "V", "setAutoCommit", commit_automatically, check = FALSE)))
    }
    
    escaped_table_name <- sql_escape(name, TRUE, quote_string(conn))

    if (!append) {
      data_types <- sapply(value, dbDataType, dbObj = conn)
      field_definitions <- paste(sql_escape(names(value), TRUE, quote_string(conn)), data_types, collapse = ', ')
      statement <- sprintf("CREATE TABLE %s (%s)", escaped_table_name, field_definitions)
      table_was_created <- dbSendUpdate(conn, statement)
      if (!table_was_created) {
        stop("Table could not be created")
      }
    }
    
    if (nrow(value) > 0) {
      statement <- sprintf("INSERT INTO %s(%s) VALUES(%s)",
        escaped_table_name,
        paste(sql_escape(names(value), TRUE, quote_string(conn)), collapse = ', '),
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

#' Commit a transaction.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbCommit", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "commit", check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Roll back a transaction.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbRollback", signature(conn = "JDBCConnection"), 
  function(conn, ...) {
    jtry(.jcall(conn@j_connection, "V", "rollback", check = FALSE))
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' @export
setMethod("summary", "JDBCConnection", 
  function(object, ...) {
    info <- connection_info(object@j_connection)
    cat("JDBC Connection\n")
    cat(sprintf("  URL: %s\n", info$url))
    cat(sprintf("  User: %s\n", info$user_name)) 
    cat(sprintf("  Driver name: %s\n", info$driver_name))
    cat(sprintf("  Driver Version: %s\n", info$driver_version))   
    cat(sprintf("  Database product name: %s\n", info$database_product_name))
    cat(sprintf("  Database product version: %s\n", info$database_product_version))
    invisible(NULL)
  }
)

#' Get info about the connection.
#' 
#' @param dbObj an object of class \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Needed for compatiblity with generic.
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
    user_name = jtry(.jcall(md, "S", "getUserName", check = FALSE)),
    quote_string = jtry(.jcall(md, "S", "getIdentifierQuoteString", check = FALSE))
  )
}

#' Get the last exception from the connection.
#' 
#' @param conn an object of class \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetException", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    warning("Not implemented. Returns an empty list.")
    list()
  },
  valueClass = "list"
)

#' List available JDBC result sets.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbListResults", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    warning("JDBC maintains no list of active results")
    list()
  },
  valueClass = "list"
)

#' @export
setMethod("SQLKeywords", signature(dbObj = "JDBCObject"),
  function(dbObj, ...) {
    md <- jtry(.jcall(dbObj@j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE))
    keywords <- jtry(.jcall(md, "S", "getSQLKeywords", check = FALSE))
    unique(c(unlist(strsplit(keywords, "\\s*,\\s*")), .SQL92Keywords)) # TODO Java API refers to SQL:2003 keywords
  },
  valueClass = "character"
)

quote_string <- function(conn) {
  conn@quote_string
}
