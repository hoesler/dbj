#' @include JDBCObject.R
#' @include JDBCConnectionExtensions.R
NULL

#' Class JDBCConnection
#'
#' @docType class
#' @name JDBCConnection-class
#' @exportClass JDBCConnection
setClass("JDBCConnection", contains = c("DBIConnection", "JDBCObject"), slots = c(jc="jobjRef", identifier.quote="character"))

#' Create a JDBC connection.
#'
#' @param drv a JDBCConnection object
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbConnect", signature(drv = "JDBCConnection"),
  function(drv, ...) {
    stop("Not implemented")
  }
)

#' Calls a stored procedure.
#'
#' @param conn a JDBCConnection object
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbCallProc", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    stop("Not implemented")
  }
)

#' Disconnect an JDBC connection.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbDisconnect", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    .jcall(conn@jc, "V", "close");
    TRUE
  },
  valueClass = "logical"
)

.fillStatementParameters <- function(s, l) {
  for (i in 1:length(l)) {
    v <- l[[i]]
    if (is.na(v)) { # map NAs to NULLs (courtesy of Axel Klenk)
      sqlType <- if (is.integer(v)) 4 else if (is.numeric(v)) 8 else 12
      .jcall(s, "V", "setNull", i, as.integer(sqlType))
    } else if (is.integer(v))
      .jcall(s, "V", "setInt", i, v[1])
    else if (is.numeric(v))
      .jcall(s, "V", "setDouble", i, as.double(v)[1])
    else
      .jcall(s, "V", "setString", i, as.character(v)[1])
  }
}

#' Execute a SQL statement on a database connection
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send over the connection
#' @param ... statment parameters
#' @param list a list of statment parameters
#' @export
setMethod("dbSendQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, ..., list = NULL) {
    statement <- as.character(statement)[1L]
    ## if the statement starts with {call or {?= call then we use CallableStatement 
    if (isTRUE(as.logical(grepl("^\\{(call|\\?= *call)", statement)))) {
      s <- .jcall(conn@jc, "Ljava/sql/CallableStatement;", "prepareCall", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC callable statement ",statement)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
    } else if (length(list(...)) || length(list)) { ## use prepared statements if there are additional arguments
      s <- .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC prepared statement ", statement)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
    } else { ## otherwise use a simple statement some DBs fail with the above)
      s <- .jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
      .verify.JDBC.result(s, "Unable to create simple JDBC statement ",statement)
      r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(statement)[1], check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
    } 
    md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC result set meta data for ",statement, " in dbSendQuery")
    new("JDBCResult", jr=r, md=md, stat=s, pull=.jnull())
  }
)

#' Execute a SQL statement on a database connection to update the database
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send over the connection
#' @param ... statment parameters
#' @param list a list of statment parameters
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, ..., list=NULL) {
    statement <- as.character(statement)[1L]
    ## if the statement starts with {call or {?= call then we use CallableStatement 
    if (isTRUE(as.logical(grepl("^\\{(call|\\?= *call)", statement)))) {
      s <- .jcall(conn@jc, "Ljava/sql/CallableStatement;", "prepareCall", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC callable statement ",statement)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
    } else if (length(list(...)) || length(list)) { ## use prepared statements if there are additional arguments
      s <- .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC prepared statement ", statement)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      .jcall(s, "I", "executeUpdate", check=FALSE)
    } else {
      s <- .jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
      .verify.JDBC.result(s, "Unable to create JDBC statement ",statement)
      on.exit(.jcall(s, "V", "close")) # in theory this is not necesary since 's' will go away and be collected, but appearently it may be too late for Oracle (ORA-01000)
      .jcall(s, "I", "executeUpdate", as.character(statement)[1], check=FALSE)
    }
    x <- .jgetEx(TRUE)
    if (!is.jnull(x)) stop("execute JDBC update query failed in dbSendUpdate (", .jcall(x, "S", "getMessage"),")")
    TRUE
  },
  valueClass ="logical"
)

#' Execute a SQL statement on a database connection to fetch data from the database
#'
#' @param conn an object of class \code{\linkS4class{JDBCConnection}}
#' @param statement the statement to send
#' @param ... Ignored. Needed for compatiblity with generic.
#' @export
setMethod("dbGetQuery", signature(conn = "JDBCConnection", statement = "character"),
  function(conn, statement, ...) {
    r <- dbSendQuery(conn, statement, ...)
    ## Teradata needs this - closing the statement also closes the result set according to Java docs
    on.exit(.jcall(r@stat, "V", "close"))
    fetch(r, -1)
  }
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
  function(dbObj, ...) list()
)

#' List available JDBC result sets.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbListResults", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    warning("JDBC maintains no list of active results")
    NULL
  }
)

.fetch.result <- function(r) {
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  .verify.JDBC.result(md, "Unable to retrieve JDBC result set meta data")
  res <- new("JDBCResult", jr=r, md=md, stat=.jnull(), pull=.jnull())
  fetch(res, -1)
}

#' List available JDBC tables.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @param pattern the pattern passed to the java method ResultSet.getTables()
#' @export
setMethod("dbListTables", signature(conn = "JDBCConnection"),
  function(conn, pattern="%", ...) {
    md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC database metadata")
    r <- .jcall(md, "Ljava/sql/ResultSet;", "getTables", .jnull("java/lang/String"),
                .jnull("java/lang/String"), pattern, .jnull("[Ljava/lang/String;"), check=FALSE)
    .verify.JDBC.result(r, "Unable to retrieve JDBC tables list")
    on.exit(.jcall(r, "V", "close"))
    ts <- character()
    while (.jcall(r, "Z", "next"))
      ts <- c(ts, .jcall(r, "S", "getString", "TABLE_NAME"))
    ts
  }
)

#' Get a description of the tables available in the given catalog. 
#'
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @param pattern the pattern for table names
#' @export
setMethod("dbGetTables", signature(conn = "JDBCConnection"),
  function(conn, ..., pattern="%") {
    md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC database metadata")
    # getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
    r <- .jcall(md, "Ljava/sql/ResultSet;", "getTables", .jnull("java/lang/String"),
                .jnull("java/lang/String"), pattern, .jnull("[Ljava/lang/String;"), check=FALSE)
    .verify.JDBC.result(r, "Unable to retrieve JDBC tables list")
    on.exit(.jcall(r, "V", "close"))
    .fetch.result(r)
  }
)

#' Does the table exist?
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of table
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbExistsTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    length(dbListTables(conn, name)) > 0
  }
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
  }
)

#' Get a description of table columns available in the specified catalog.
#'
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param name character vector of length 1 giving name of the table
#' @param ... Ignored. Included for compatibility with generic.
#' @param pattern the pattern for the columns to list
#' @export
setMethod("dbListFields", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ..., pattern="%") {
    md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC database metadata")
    r <- .jcall(md, "Ljava/sql/ResultSet;", "getColumns", .jnull("java/lang/String"),
                .jnull("java/lang/String"), name, pattern, check=FALSE)
    .verify.JDBC.result(r, "Unable to retrieve JDBC columns list for ",name)
    on.exit(.jcall(r, "V", "close"))
    ts <- character()
    while (.jcall(r, "Z", "next"))
      ts <- c(ts, .jcall(r, "S", "getString", "COLUMN_NAME"))
    .jcall(r, "V", "close")
    ts
  }
)

#' Get description of table columns available in the specified catalog.
#'
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @param name the pattern for table names
#' @param pattern the pattern for column names
#' @export
setMethod("dbGetFields", signature(conn = "JDBCConnection"),
  function(conn, ..., name, pattern="%") {
    md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC database metadata")
    # getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    r <- .jcall(md, "Ljava/sql/ResultSet;", "getColumns",
      .jnull("java/lang/String"), .jnull("java/lang/String"), name, pattern, check=FALSE)
    .verify.JDBC.result(r, "Unable to retrieve JDBC columns list for ",name)
    on.exit(.jcall(r, "V", "close"))
    .fetch.result(r)
  }
)

#' Convenience functions for importing/exporting DBMS tables
#' @param conn a \code{\linkS4class{JDBCConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param ... Ignored. Needed for compatibility with generic.
setMethod("dbReadTable", signature(conn = "JDBCConnection", name = "character"),
  function(conn, name, ...) {
    dbGetQuery(conn, paste("SELECT * FROM",.sql.qescape(name,TRUE,conn@identifier.quote)))
  }
)

.sql.qescape <- function(s, identifier=FALSE, quote="\"") {
  s <- as.character(s)
  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$",s)
    if (length(s[-vid])) {
      if (is.na(quote)) stop("The JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",paste(s[-vid],collapse=','),")")
      s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
    }
    return(s)
  }
  if (is.na(quote)) quote <- ''
  s <- gsub("\\\\","\\\\\\\\",s)
  if (nchar(quote)) s <- gsub(paste("\\",quote,sep=''),paste("\\\\\\",quote,sep=''),s,perl=TRUE)
  paste(quote,s,quote,sep='')
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
  function(conn, name, value, overwrite=TRUE, append=FALSE, ...) {
    ac <- .jcall(conn@jc, "Z", "getAutoCommit")
    overwrite <- isTRUE(as.logical(overwrite))
    append <- if (overwrite) FALSE else isTRUE(as.logical(append))
    if (is.vector(value) && !is.list(value)) value <- data.frame(x=value)
    if (length(value)<1) stop("value must have at least one column")
    if (is.null(names(value))) names(value) <- paste("V",1:length(value),sep='')
    if (length(value[[1]])>0) {
      if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]))
    } else {
      if (!is.data.frame(value)) value <- as.data.frame(value)
    }
    fts <- sapply(value, dbDataType, dbObj=conn)
    if (dbExistsTable(conn, name)) {
      if (overwrite) dbRemoveTable(conn, name)
      else if (!append) stop("Table `",name,"' already exists")
    } else if (append) stop("Cannot append to a non-existing table `",name,"'")
    fdef <- paste(.sql.qescape(names(value), TRUE, conn@identifier.quote),fts,collapse=',')
    qname <- .sql.qescape(name, TRUE, conn@identifier.quote)
    if (ac) {
      .jcall(conn@jc, "V", "setAutoCommit", FALSE)
      on.exit(.jcall(conn@jc, "V", "setAutoCommit", ac))
    }
    if (!append) {
      ct <- paste("CREATE TABLE ",qname," (",fdef,")",sep= '')
      dbSendUpdate(conn, ct)
    }
    if (length(value[[1]])) {
      inss <- paste("INSERT INTO ",qname," VALUES(", paste(rep("?",length(value)),collapse=','),")",sep='')
      for (j in 1:length(value[[1]]))
        dbSendUpdate(conn, inss, list=as.list(value[j,]))
    }
    if (ac) dbCommit(conn)            
  }
)

#' Commit a transaction.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbCommit", signature(conn = "JDBCConnection"),
  function(conn, ...) {
    .jcall(conn@jc, "V", "commit")
    TRUE
  }
)

#' Roll back a transaction.
#' 
#' @param conn An existing \code{\linkS4class{JDBCConnection}}
#' @param ... Ignored. Included for compatibility with generic.
#' @export
setMethod("dbRollback", signature(conn = "JDBCConnection"), 
  function(conn, ...) {
    .jcall(conn@jc, "V", "rollback")
    TRUE
  }
)

#' @export
setMethod("summary", "JDBCConnection", 
  function(object, ...) {
    stop("Not implemented")
  }
)
