#' @include JDBCObject.R
#' @include JDBCConnectionExtensions.R
#' @include JavaUtils.R
NULL

#' Class JDBCConnection
#'
#' @docType class
#' @name JDBCConnection-class
#' @exportClass JDBCConnection
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

# Get the corresponding int value (java.sql.Types) for the given object type
as.sqlType <- function(object) {
  if (is.integer(object)) 4 # INTEGER
  else if (is.numeric(object)) 8 # DOUBLE
  else 12 # VARCHAR
}

insert_parameters <- function(j_statement, parameter_list) {
  if (length(parameter_list) > 0) {
    for (i in seq(length(parameter_list))) {
      parameter <- parameter_list[[i]]
      if (is.na(parameter)) { # map NAs to NULLs (courtesy of Axel Klenk)
        sqlType <- as.sqlType(parameter)
        jtry(.jcall(j_statement, "V", "setNull", i, as.integer(sqlType)))
      } else if (is.integer(parameter))
        jtry(.jcall(j_statement, "V", "setInt", i, parameter[1]))
      else if (is.numeric(parameter))
        jtry(.jcall(j_statement, "V", "setDouble", i, as.double(parameter)[1]))
      else
        jtry(.jcall(j_statement, "V", "setString", i, as.character(parameter)[1]))
    }
  }
}

prepareCall <- function(conn, statement, parameters) {
  j_statement <- jtry(.jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareCall", statement, check = FALSE))
  insert_parameters(j_statement, parameters)
  j_statement
}

prepareStatement <- function(conn, statement, parameters) {
  j_statement <- jtry(.jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check = FALSE))
  insert_parameters(j_statement, parameters)
  j_statement
}

executeQuery <- function(j_statement) {
  jtry(.jcall(j_statement, "Ljava/sql/ResultSet;", "executeQuery", check = FALSE))
}

executeUpdate <- function(j_statement) {
  jtry(.jcall(j_statement, "I", "executeUpdate", check = FALSE))
}

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
    if (isTRUE(as.logical(grepl("^\\{(call|\\? = *call)", statement)))) {
      j_statement <- prepareCall(conn, statement, parameters)
      j_result_set <- executeQuery(j_statement)
    } else {
      j_statement <- prepareStatement(conn, statement, parameters)
      j_result_set <- executeQuery(j_statement)
    } 
    
    new("JDBCResult",
      jr = j_result_set
    )
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
    if (isTRUE(as.logical(grepl("^\\{(call|\\? = *call)", statement)))) {
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

.fetch.result <- function(r, close = FALSE) {
  if (close) {
    on.exit(jtry(.jcall(r, "V", "close")))
  }
  res <- new("JDBCResult", jr = r)
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
    r <- jtry(.jcall(md, "Ljava/sql/ResultSet;", "getTables", .jnull("java/lang/String"),
                .jnull("java/lang/String"), pattern, .jnull("[Ljava/lang/String;"), check = FALSE),
      jstop, "Unable to retrieve JDBC tables list")    
    .fetch.result(r, close = TRUE)
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
    r <- jtry(.jcall(md, "Ljava/sql/ResultSet;", "getColumns",
        .jnull("java/lang/String"), .jnull("java/lang/String"), name, pattern, check = FALSE),
      jstop, "Unable to retrieve JDBC columns list for ", name)
    .fetch.result(r, close = TRUE)
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
    dbGetQuery(conn, paste("SELECT * FROM",.sql.qescape(name,TRUE,conn@identifier.quote)))
  },
  valueClass = "data.frame"
)

.sql.qescape <- function(s, identifier = FALSE, quote = "\"") {
  s <- as.character(s)
  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$",s)
    if (length(s[-vid])) {
      if (is.na(quote)) {
        stop("The JDBC connection doesn't support quoted identifiers, 
          but table/column name contains characters that must be quoted (", paste(s[-vid], collapse = ','), ")")
      }
      s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
    }
    return(s)
  }
  if (is.na(quote)) quote <- ''
  s <- gsub("\\\\","\\\\\\\\",s)
  if (nchar(quote)) s <- gsub(paste("\\",quote,sep = ''),paste("\\\\\\",quote,sep = ''),s,perl = TRUE)
  paste(quote,s,quote,sep = '')
}

#' Write a local data frame or file to the database.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of table to write to
#' @param value the date frame to write to the table
#' @param overwrite a logical value indicating if the table should be overwritten if it exists
#' @param append a logical value indicating if the data shuld get appended to an existing table
#' @export
setMethod("dbWriteTable", signature(conn = "JDBCConnection", name = "character", value = "data.frame"),
  function(conn, name, value, overwrite = TRUE, append = FALSE, ...) {
    ac <- jtry(.jcall(conn@jc, "Z", "getAutoCommit", check = FALSE))
    overwrite <- isTRUE(as.logical(overwrite))
    append <- if (overwrite) FALSE else isTRUE(as.logical(append))
    if (is.vector(value) && !is.list(value)) value <- data.frame(x = value)
    if (length(value)<1) stop("value must have at least one column")
    if (is.null(names(value))) names(value) <- paste("V",1:length(value),sep = '')
    if (length(value[[1]])>0) {
      if (!is.data.frame(value)) value <- as.data.frame(value, row.names = 1:length(value[[1]]))
    } else {
      if (!is.data.frame(value)) value <- as.data.frame(value)
    }
    fts <- sapply(value, dbDataType, dbObj = conn)
    if (dbExistsTable(conn, name)) {
      if (overwrite) dbRemoveTable(conn, name)
      else if (!append) stop("Table `",name,"' already exists")
    } else if (append) stop("Cannot append to a non-existing table `",name,"'")
    fdef <- paste(.sql.qescape(names(value), TRUE, conn@identifier.quote),fts,collapse = ',')
    qname <- .sql.qescape(name, TRUE, conn@identifier.quote)
    if (ac) {
      jtry(.jcall(conn@jc, "V", "setAutoCommit", FALSE, check = FALSE))
      on.exit(jtry(.jcall(conn@jc, "V", "setAutoCommit", ac, check = FALSE)))
    }
    if (!append) {
      ct <- paste("CREATE TABLE ",qname," (",fdef,")",sep = '')
      dbSendUpdate(conn, ct)
    }
    if (length(value[[1]])) {
      inss <- paste("INSERT INTO ",qname," VALUES(", paste(rep("?",length(value)),collapse = ','),")",sep = '')
      for (j in 1:length(value[[1]]))
        dbSendUpdate(conn, inss, parameters = as.list(value[j,]))
    }
    if (ac) dbCommit(conn)            
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
