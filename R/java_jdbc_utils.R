#' @include java_utils.R
#' @include java_sql_types.R
#' @include type_mapping.R
NULL

jdbc_parse_url <- function(url) {
  matches <- regmatches(url, regexec("^jdbc:([^:]+):(.+)", url))
  if (length(matches[[1]]) == 0) {
    stop("%s is not a valid JDBC URL")
  } else {
    return(lapply(matches, function(x) list(subprotocol = x[2], subname = x[3])))
  }
}

#' Register a JDBC Driver
#' 
#' @param driver_class a character vector specifying the JDBC driver classes (e.g. 'org.h2.Driver')
#' @param classpath a character vector of length one specifying classpaths separated by \code{\link[=.Platform]{path.sep}}
#'  or a character vector of classpaths which will be added to the \code{\link[=.jaddClassPath]{rJava class loader}}
#' 
#' @export
jdbc_register_driver <- function(driver_class, classpath = NULL) {
  if (!is.null(classpath)) {
    ## expand all paths in the classpath
    expanded_paths <- path.expand(unlist(strsplit(classpath, .Platform$path.sep)))
    .jaddClassPath(expanded_paths)
  }

  for (i in seq_along(driver_class)) {
    class_name <- driver_class[i]
    tryCatch(
      .jfindClass(as.character(class_name)),
      error = function(e) sprintf("Driver for class '%s' could not be found.", class_name)
    )
  }  
}

#' Create a Java JDBC Driver object
#' 
#' @inheritParams jdbc_register_driver
#' @export
#' @keywords internal
create_jdbc_driver <- function(driver_class, classpath = NULL) {
  jdbc_register_driver(driver_class, classpath)

  j_drv <- .jnew(driver_class)
  verifyNotNull(j_drv)

  j_drv
}

#' Establish a JDBC Connection
#' 
#' @param url the URL of the form \code{jdbc:subprotocol:subname}
#' @param user the user to log in
#' @param password the user's password
#' @param ... additional connection arguments
#' @return a jObjRef referencing a java.sql.Connection
#' @keywords internal
create_jdbc_connection <- function(url, user, password, ...) {
  j_properties <- jtry(jnew("java/util/Properties"))    
  properties <- c(user = user, password = password, list(...))
  for (key in names(properties)) {
    value <- as.character(properties[[key]])
    jtry(jcall(j_properties, "Ljava/lang/Object;", "setProperty", key, value))
  } 
  jtry(jcall("java/sql/DriverManager", "Ljava/sql/Connection;", "getConnection", url, j_properties))
}

#' Set the values of prepared statement.
#' 
#' @param  j_statement a Java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
#' @param write_conversions a list of \code{\link{write_conversion_rule}} objects
#' @keywords internal
insert_parameters <- function(j_statement, parameter_list, write_conversions) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.list(parameter_list))
  if (length(parameter_list) > 0) {
    j_table <- create_j_table(as.data.frame(parameter_list, stringsAsFactors = FALSE), write_conversions)

    jtry(jcall("com/github/hoesler/dbj/PreparedStatements", "V", "insert",
      .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "com/github/hoesler/dbj/Table"), as.integer(0)))
  }
  invisible(NULL)
}

create_prepared_statement <- function(conn, statement,
  result_set_type = RESULT_SET_TYPE$TYPE_FORWARD_ONLY,
  result_set_concurrency = RESULT_SET_CONCURRENCY$CONCUR_READ_ONLY) {
  ## if the statement starts with {call or {? = call then we use CallableStatement 
  if (any(grepl("^\\{(call|\\? = *call)", statement))) {
    return(prepare_call(conn, statement, result_set_type, result_set_concurrency))
  } else {
    return(prepare_statement(conn, statement, result_set_type, result_set_concurrency))
  } 
}

prepare_call <- function(conn, statement,
  result_set_type = RESULT_SET_TYPE$TYPE_FORWARD_ONLY,
  result_set_concurrency = RESULT_SET_CONCURRENCY$CONCUR_READ_ONLY) {

  assert_that(is.character(statement))
  assert_that(is.integer(result_set_type))
  assert_that(is.integer(result_set_concurrency))

  jtry(jcall(conn@j_connection, "Ljava/sql/CallableStatement;", "prepareCall",
    statement, result_set_type, result_set_concurrency))
}

prepare_statement <- function(conn, statement,
  result_set_type = RESULT_SET_TYPE$TYPE_FORWARD_ONLY,
  result_set_concurrency = RESULT_SET_CONCURRENCY$CONCUR_READ_ONLY) {

  assert_that(is.character(statement))
  assert_that(is.integer(result_set_type))
  assert_that(is.integer(result_set_concurrency))
  
  jtry(.jcall(conn@j_connection, "Ljava/sql/PreparedStatement;", "prepareStatement",
    statement, result_set_type, result_set_concurrency, check = FALSE))
}

execute_query <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(jcall(j_statement, "Z", "execute"))
}

execute_update <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(jcall(j_statement, "I", "executeUpdate"))
}

add_batch <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(jcall(j_statement, "V", "addBatch"))
}

#' Transform a data frame into a Java reference to a com/github/hoesler/dbj/Table
#' 
#' @param data a data.frame
#' @param write_conversions a list of \code{\link{write_conversion_rule}} objects
#' @keywords internal
create_j_table <- function(data, write_conversions) {
  assert_that(is.data.frame(data))

  j_columns <- unlist(lapply(seq_along(data), function(column_index) {
    column_data <- data[,column_index]
    create_j_colum(column_data, write_conversions)
  }))

  jtry(.jcall("com/github/hoesler/dbj/ArrayListTable", "Lcom/github/hoesler/dbj/ArrayListTable;",
    "create", .jarray(j_columns, contents.class = "com/github/hoesler/dbj/Column"), check = FALSE))
}

#' Create a Java Column class for given column_data
#' 
#' @param column_data the data to insert
#' @param write_conversions a list of \code{\link{write_conversion_rule}} objects
#' @keywords internal
create_j_colum <- function(column_data, write_conversions) {
  conversion_rule <- find_conversion_rule(write_conversions, column_data, list())
  converted_column_data <- do.call(conversion_rule$conversion, list(data = column_data))
  db_data_type <- do.call(conversion_rule$create_type, list(data = column_data))

  sql_type <- as.jdbc_sql_type(db_data_type)

  j_column_classname <- NULL
  j_column_data <- NULL

  with(JDBC_SQL_TYPES,
    if (sql_type %in% c(BIT, BOOLEAN)) {
      j_column_classname <<- "com/github/hoesler/dbj/BooleanColumn"
      j_column_data <<- as.logical(converted_column_data)
    } else if (sql_type %in% c(TINYINT, SMALLINT, INTEGER)) {
      j_column_classname <<- "com/github/hoesler/dbj/IntegerColumn"
      j_column_data <<- as.integer(converted_column_data)
    } else if (sql_type %in% c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL)) {
      j_column_classname <<- "com/github/hoesler/dbj/DoubleColumn"
      j_column_data <<- as.numeric(converted_column_data)
    } else if (sql_type %in% c(BIGINT, DATE, TIME, TIMESTAMP)) {
      j_column_classname <<- "com/github/hoesler/dbj/LongColumn"
      j_column_data <<- .jlong(as.numeric(converted_column_data))
    } else if (sql_type %in% c(VARCHAR, CHAR, LONGVARCHAR, NVARCHAR, NCHAR, LONGNVARCHAR)) {
      j_column_classname <<- "com/github/hoesler/dbj/StringColumn"
      j_column_data <<- as.character(converted_column_data)
    } else if (sql_type %in% c(BINARY, BLOB)) {
      j_column_classname <<- "com/github/hoesler/dbj/BinaryColumn"
      j_column_data <<- lapply(converted_column_data, .jarray)
    } else {
      stop("Unsupported SQL type '", sql_type, "'")
    }
  )

  assert_that(all(!is.null(c(j_column_classname, j_column_data))))    
  
  j_column <- jtry(jcall(j_column_classname, sprintf("L%s;", j_column_classname),
      "create", as.integer(sql_type), .jarray(j_column_data), .jarray(is.na(column_data))))

  return(j_column)
}

batch_insert <- function(j_statement, data, write_conversions) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_table <- create_j_table(data, write_conversions)

  jtry(.jcall("com/github/hoesler/dbj/PreparedStatements", "V", "batchInsert",
    .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "com/github/hoesler/dbj/Table"), check = FALSE))
}

#' Execute a batch statement
#' 
#' @param  j_statement a jobjRef object to a java.sql.PreparedStatement object
#' @return integer vector with update counts containing one element for each command in the batch
#' @keywords internal
execute_batch <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(jcall(j_statement, "[I", "executeBatch"))
}

#' Create a new ResultSetMetaData reference object
#' @param j_result_set a jobjRef object to a java.sql.ResultSet object
#' @keywords internal
get_meta_data <- function(j_result_set) {
  #assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  j_meta_data <- jtry(jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData"))
  verifyNotNull(j_meta_data, "Unable to retrieve JDBC result set meta data for ", j_result_set, " in dbSendQuery")
  return(j_meta_data)
}

close_statement <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "V", "close"))
}

close_result_set <- function(j_result_set) {
  #assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  jtry(.jcall(j_result_set, "V", "close"))
}

jdbc_connection_is_closed <- function(j_connection) jtry(jcall(j_connection, "Z", "isClosed"))

jdbc_close_connection <- function(j_connection) jtry(jcall(j_connection, "V", "close"))

jdbc_connection_info <- function(j_connection) {
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

jdbc_connection_is_valid <- function(j_connection, timeout) jtry(jcall(j_connection, "Z", "isValid", as.integer(timeout)))

jdbc_get_result_set <- function(j_statement) jtry(jcall(j_statement, "Ljava/sql/ResultSet;", "getResultSet"))

jdbc_get_update_count <- function(j_statement) jtry(jcall(j_statement, "I", "getUpdateCount"))

jdbc_get_database_meta <- function(j_connection) jtry(jcall(j_connection, "Ljava/sql/DatabaseMetaData;", "getMetaData"),
      jstop, "Failed to retrieve JDBC database metadata")

jdbc_dbmeta_get_tables <- function(j_database_data, trable_name_pattern) {
  # getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
  jtry(
    jcall(j_database_data, "Ljava/sql/ResultSet;", "getTables",
      .jnull("java/lang/String"),
      .jnull("java/lang/String"),
      trable_name_pattern,
      .jnull("[Ljava/lang/String;")),
    jstop, "Unable to retrieve JDBC tables list")
}

jdbc_dbmeta_get_columns <- function(j_database_meta, table_name, column_name_pattern) {
  # getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
  jtry(
    jcall(j_database_meta, "Ljava/sql/ResultSet;", "getColumns",
      .jnull("java/lang/String"),
      .jnull("java/lang/String"),
      table_name,
      column_name_pattern),
    jstop, "Unable to retrieve JDBC columns list for ", table_name)
}

jdbc_connection_set_savepoint <- function(j_connection, savepoint_name) jtry(jcall(j_connection, "Ljava/sql/Savepoint;", "setSavepoint", savepoint_name))

jdbc_connection_commit <- function(j_connection) jtry(jcall(j_connection, "V", "commit"))

jdbc_connection_autocommit <- function(j_connection, auto) jtry(jcall(j_connection, "V", "setAutoCommit", auto))

jdbc_connection_rollback <- function(j_connection, j_savepoint = NULL) {
  if (is.null(j_savepoint)) {
    jtry(jcall(j_connection, "V", "rollback"))
  } else {
    jtry(jcall(j_connection, "V", "rollback", j_savepoint))
  } 
}

jdbc_rsmeta_column_count <- function(j_result_set_meta) jtry(jcall(j_result_set_meta, "I", "getColumnCount"))

jdbc_rsmeta_column_name <- function(j_result_set_meta, column_index) jtry(jcall(j_result_set_meta, "S", "getColumnName", column_index))

jdbc_rsmeta_column_type <- function(j_result_set_meta, column_index) jtry(jcall(j_result_set_meta, "I", "getColumnType", column_index))

jdbc_rsmeta_column_typename <- function(j_result_set_meta, column_index) jtry(jcall(j_result_set_meta, "S", "getColumnTypeName", column_index))

jdbc_rsmeta_column_label <- function(j_result_set_meta, column_index) jtry(jcall(j_result_set_meta, "S", "getColumnLabel", column_index))

# 0 = disallows NULL, 1 = allows NULL, 2 = unknown
jdbc_rsmeta_column_nullable <- function(j_result_set_meta, column_index) jtry(jcall(j_result_set_meta, "I", "isNullable", column_index))

jdbc_result_set_is_closed <- function(j_result_set) jtry(jcall(j_result_set, "Z", "isClosed"))

jdbc_result_set_get_statement <- function(j_result_set) jtry(jcall(j_result_set, "Ljava/sql/Statement;", "getStatement"))

jdbc_driver_major_version <- function(j_drv) jtry(jcall(j_drv, "I", "getMajorVersion"))

jdbc_driver_minor_version <- function(j_drv) jtry(jcall(j_drv, "I", "getMinorVersion"))
