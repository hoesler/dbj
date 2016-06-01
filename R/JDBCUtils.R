#' @include JavaUtils.R
#' @include JDBCSQLTypes.R
#' @include JDBCMapping.R
NULL

#' Create a Java JDBC Driver object
#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by \code{path.sep} variable in \code{\link{.Platform}} which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @export
create_jdbc_driver <- function(driverClass, classPath) {
  assert_that(is.character(driverClass))
  assert_that(is.character(classPath))
  
  ## expand all paths in the classPath
  expanded_paths <- path.expand(unlist(strsplit(classPath, .Platform$path.sep)))
  .jaddClassPath(expanded_paths)
  
  tryCatch(.jfindClass(as.character(driverClass)[1]),
    error = function(e) sprintf("Driver for class '%s' could not be found.", driverClass))

  j_drv <- .jnew(driverClass)
  verifyNotNull(j_drv)

  j_drv
}

#' Create a Java JDBC Connection object
#' @param j_drv The Java driver object
#' @param url the url to connect to
#' @param user the user to log in
#' @param password the users password
#' @param ... named values which get transformed into key-value pairs of a 
#'            Java Properties object which is passed to the connect method.
#' @export
create_jdbc_connection <- function(j_drv, url, user, password, ...) {
  j_con <- jtry(
    .jcall("java/sql/DriverManager", "Ljava/sql/Connection;", "getConnection",
      as.character(url)[1], as.character(user)[1], as.character(password)[1],
      check = FALSE),
    onError = function(j_exception, expression, ...) {
      message <- .jcall(j_exception, "S", "toString")
      warning("Failed to connect: ", message)
    }
  )
  
  if (is.jnull(j_con) && !is.jnull(j_drv)) {
    # ok one reason for this to fail is its interaction with rJava's
    # class loader. In that case we try to load the driver directly.
    oex <- .jgetEx(TRUE)
    p <- .jnew("java/util/Properties")
    if (length(user) == 1 && nchar(user)) .jcall(p, "Ljava/lang/Object;", "setProperty", "user",user)
    if (length(password) == 1 && nchar(password)) .jcall(p, "Ljava/lang/Object;", "setProperty", "password",password)
    l <- list(...)
    if (length(names(l))) for (n in names(l)) .jcall(p, "Ljava/lang/Object;", "setProperty", n, as.character(l[[n]]))
    j_con <- jtry(.jcall(j_drv, "Ljava/sql/Connection;", "connect", as.character(url)[1], p, check = FALSE))
  }

  verifyNotNull(j_con, "Unable to connect JDBC to ", url)

  j_con
}

#' Set the values of prepared statment.
#' 
#' @param  j_statement a java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
#' @param write_conversions a list of \code{JDBCWriteConversion} objects
#' @keywords internal
insert_parameters <- function(j_statement, parameter_list, write_conversions) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.list(parameter_list))
  if (length(parameter_list) > 0) {
    j_table <- create_j_table(j_statement, as.data.frame(parameter_list, stringsAsFactors = FALSE), write_conversions)

    jtry(.jcall("com/github/hoesler/dbj/PreparedStatements", "V", "insert",
      .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "com/github/hoesler/dbj/Table"), as.integer(0), check = FALSE))
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

  jtry(.jcall(conn@j_connection, "Ljava/sql/CallableStatement;", "prepareCall",
    statement, result_set_type, result_set_concurrency, check = FALSE))
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
  jtry(.jcall(j_statement, "Z", "execute", check = FALSE))
}

execute_update <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "I", "executeUpdate", check = FALSE))
}

add_batch <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(.jcall(j_statement, "V", "addBatch", check = FALSE))
}

#' Transform a data frame into a java reference to a com/github/hoesler/dbj/Table
#' 
#' @param j_statement a jobjRef to a java.sql.PreparedStatement
#' @param data a data.frame
#' @param write_conversions a list of JDBCWriteConversion objects
#' @keywords internal
create_j_table <- function(j_statement, data, write_conversions) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_statement_meta <- jtry(.jcall(j_statement, "Ljava/sql/ParameterMetaData;", "getParameterMetaData", check = FALSE))

  j_columns <- unlist(lapply(seq_along(data), function(column_index) {
    column_data <- data[,column_index]
    sql_type <- jtry(.jcall(j_statement_meta, "I", "getParameterType", column_index, check = FALSE))
    is_nullable <- jtry(.jcall(j_statement_meta, "I", "isNullable", column_index, check = FALSE))
    if (is_nullable == 0 && any(is.na(column_data))) {
      stop("Column is not nullable but data contains NA")
    }
    create_j_colum(column_data, sql_type, is_nullable, write_conversions)
  }))

  jtry(.jcall("com/github/hoesler/dbj/ArrayListTable", "Lcom/github/hoesler/dbj/ArrayListTable;",
    "create", .jarray(j_columns, contents.class = "com/github/hoesler/dbj/Column"), check = FALSE))
}

#' Create a Java Column class for given column_data
#' 
#' @param column_data the data to insert
#' @param sql_type the type of the column
#' @param is_nullable is the column nullable? 0 = disallows NULL, 1 = allows NULL, 2 = unknown
#' @param write_conversions a list of JDBCWriteConversion objects
create_j_colum <- function(column_data, sql_type, is_nullable, write_conversions) {
  converted_column_data <- convert_to_transfer(write_conversions, column_data, list(sql_type = sql_type, class_names = class(column_data)))
  
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
  
  j_column <-
  if (is_nullable > 0 && any(is.na(column_data))) {
    jtry(.jcall(j_column_classname, sprintf("L%s;", j_column_classname),
      "create", sql_type, .jarray(j_column_data), .jarray(is.na(column_data)), check = FALSE))
  } else {
    jtry(.jcall(j_column_classname, sprintf("L%s;", j_column_classname),
      "create", sql_type, .jarray(j_column_data), check = FALSE))
  }

  return(j_column)
}

batch_insert <- function(j_statement, data, write_conversions) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_table <- create_j_table(j_statement, data, write_conversions)

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
  jtry(.jcall(j_statement, "[I", "executeBatch", check = FALSE))
}

get_meta_data <- function(j_result_set) {
  #assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  j_meta_data <- jtry(.jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE))
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
