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
  slots = c(jc = "jobjRef", identifier.quote = "character"))

#' Create a JDBC connection.
#'
#' @param drv a JDBCConnection object
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbConnect", signature(drv = "JDBCConnection"),
  function(drv, ...) {
    .NotYetImplemented()
  }
)

#' Calls a stored procedure.
#'
#' @param conn a JDBCConnection object
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbCallProc", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    .NotYetImplemented()
  }
)

#' Disconnect an JDBC connection.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbDisconnect", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    jtry(.jcall(conn@jc, "V", "close", check = FALSE))
    TRUE
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
    
    ## if the statement starts with {call or {? = call then we use CallableStatement 
    if (any(grepl("^\\{(call|\\? = *call)", statement))) {
      j_statement <- prepareCall(conn, statement, parameters)
      j_result_set <- executeQuery(j_statement)
    } else {
      j_statement <- prepareStatement(conn, statement, parameters)
      j_result_set <- executeQuery(j_statement)
    } 
    
    JDBCResult(j_result_set)
  },
  valueClass = "JDBCResult"
)

#' Execute a SQL statement on a database connection to update the database
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send over the connection
#' @param parameters a list of statment parameters
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, parameters = list(), ...) {
    statement <- as.character(statement)[1L]

    ## if the statement starts with {call or {? = call then we use CallableStatement 
    if (any(grepl("^\\{(call|\\? = *call)", statement))) {
      j_statement <- prepareCall(conn, statement, parameters)
      j_result_set <- executeQuery(j_statement)
    } else {
      j_statement <- prepareStatement(conn, statement, parameters)
      affected_rows <- executeUpdate(j_statement)
    }

    invisible(TRUE)
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
    on.exit({
      j_statement <- .jcall(result@jr, "Ljava/sql/Statement;", "getStatement")
      .jcall(j_statement, "V", "close")
    })
    fetch(result, -1)
  },
  valueClass = "data.frame"
)

#' Get the last exception from the connection.
#' 
#' @param conn an object of class \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetException", signature(conn = "JDBCConnection"),
  function(conn, ...) list(),
  valueClass = "list"
)

#' Get info about the connection.
#' 
#' @param dbObj an object of class \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCConnection"),
  function(dbObj, ...) {
    list()
  }
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
  }
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

#' Get a description of the tables available in the given catalog. 
#'
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param pattern the pattern for table names
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbGetTables", signature(conn = "JDBCConnection"),
  function(conn, pattern = "%", ...) {
    md <- jtry(.jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE),
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

#' Get description of table columns available in the specified catalog.
#'
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name the pattern for table names
#' @param pattern the pattern for column names
#' @param ... Ignored. Needed for compatibility with generic
#' @export
setMethod("dbGetFields", signature(conn = "JDBCConnection"),
  function(conn, name, pattern = "%", ...) {
    md <- jtry(.jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE),
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
setMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbGetQuery(conn, paste("SELECT * FROM", sql_escape(name,TRUE,conn@identifier.quote)))
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
    
    commit_automatically <- jtry(.jcall(conn@jc, "Z", "getAutoCommit", check = FALSE))
    if (commit_automatically) {
      jtry(.jcall(conn@jc, "V", "setAutoCommit", FALSE, check = FALSE))
      on.exit(jtry(.jcall(conn@jc, "V", "setAutoCommit", commit_automatically, check = FALSE)))
    }
    
    escaped_table_name <- sql_escape(name, TRUE, conn@identifier.quote)

    if (!append) {
      data_types <- sapply(value, dbDataType, dbObj = conn)
      field_definitions <- paste(sql_escape(names(value), TRUE, conn@identifier.quote), data_types, collapse = ', ')
      statement <- sprintf("CREATE TABLE %s (%s)", escaped_table_name, field_definitions)
      dbSendUpdate(conn, statement)
    }
    
    if (nrow(value) > 0) {
      statement <- sprintf("INSERT INTO %s(%s) VALUES(%s)",
        escaped_table_name,
        paste(sql_escape(names(value), TRUE, conn@identifier.quote), collapse = ', '),
        paste(rep("?", length(value)), collapse = ', '))
      
      apply(value, 1, function(row) {
        dbSendUpdate(conn, statement, parameters = as.list(row))
      })
    }
    
    if (commit_automatically) {
      dbCommit(conn) 
    }

    TRUE           
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
    jtry(.jcall(conn@jc, "V", "commit", check = FALSE))
    TRUE
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
    jtry(.jcall(conn@jc, "V", "rollback", check = FALSE))
    TRUE
  },
  valueClass = "logical"
)

#' @export
setMethod("summary", "JDBCConnection", 
  function(object, ...) {
    .NotYetImplemented()
  }
)
