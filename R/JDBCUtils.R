#' @include JavaUtils.R
#' @include SQLUtils.R
NULL

#' Set the values of prepared statment.
#' 
#' @param  j_statement a java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
insert_parameters <- function(j_statement, parameter_list) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.list(parameter_list))
  if (length(parameter_list) > 0) {
    j_table <- create_table(as.data.frame(parameter_list))

    jtry(.jcall("info/urbanek/Rpackage/RJDBC/PreparedStatements", "V", "insert",
      .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "info/urbanek/Rpackage/RJDBC/Table"), as.integer(0), check = FALSE))
  }
  invisible(NULL)
}

# Get the corresponding int value (java.sql.Types) for the given object type
as.sql_type <- function(object) {
  if (is.integer(object)) 4 # INTEGER
  else if (is.numeric(object)) 8 # DOUBLE
  else 12 # VARCHAR
}

create_prepared_statement <- function(conn, statement) {
  ## if the statement starts with {call or {? = call then we use CallableStatement 
  if (any(grepl("^\\{(call|\\? = *call)", statement))) {
    prepare_call(conn, statement)
  } else {
    prepare_statement(conn, statement)
  } 
}

prepare_call <- function(conn, statement) {
  jtry(.jcall(conn@j_connection, "Ljava/sql/CallableStatement;", "prepareCall", statement, check = FALSE))
}

prepare_statement <- function(conn, statement) {
  jtry(.jcall(conn@j_connection, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check = FALSE))
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

create_table <- function(data) {
  j_columns <- unlist(lapply(data, function(column) {
    if (is.integer(column)) {
      jtry(.jcall("info/urbanek/Rpackage/RJDBC/IntegerColumn", "Linfo/urbanek/Rpackage/RJDBC/IntegerColumn;",
        "create", .jarray(column), .jarray(is.na(column)), check = FALSE))
    } else if (is.numeric(column)) {
      jtry(.jcall("info/urbanek/Rpackage/RJDBC/DoubleColumn", "Linfo/urbanek/Rpackage/RJDBC/DoubleColumn;",
        "create", .jarray(column), .jarray(is.na(column)), check = FALSE))
    } else if (is(column, "Date")) {
      jtry(.jcall("info/urbanek/Rpackage/RJDBC/DateColumn", "Linfo/urbanek/Rpackage/RJDBC/DateColumn;",
        "forDays", .jarray(as.integer(column)), .jarray(is.na(column)), check = FALSE))
    } else if (is(column, "POSIXt")) {
      jtry(.jcall("info/urbanek/Rpackage/RJDBC/TimestampColumn", "Linfo/urbanek/Rpackage/RJDBC/TimestampColumn;",
        "forSeconds", .jarray(as.integer(column)), .jarray(is.na(column)), check = FALSE))
    } else {
      j_array <- .jarray(as.character(column))
      jtry(.jcall("info/urbanek/Rpackage/RJDBC/StringColumn", "Linfo/urbanek/Rpackage/RJDBC/StringColumn;",
        "create", j_array, .jarray(is.na(column)), check = FALSE))
    }
  }))

  j_table <- jtry(.jcall("info/urbanek/Rpackage/RJDBC/ArrayListTable", "Linfo/urbanek/Rpackage/RJDBC/ArrayListTable;",
    "create", .jarray(j_columns, contents.class = "info/urbanek/Rpackage/RJDBC/Column"), check = FALSE))
}

batch_insert <- function(j_statement, data) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_table <- create_table(data)

  jtry(.jcall("info/urbanek/Rpackage/RJDBC/PreparedStatements", "V", "batchInsert",
    .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "info/urbanek/Rpackage/RJDBC/Table"), check = FALSE))
}

#' Execute a batch statement
#' 
#' @param  j_statement a jobjRef object to a java.sql.PreparedStatement object
#' @return integer vector with update counts containing one element for each command in the batch
execute_batch <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(.jcall(j_statement, "[I", "executeBatch", check = FALSE))
}

get_meta_data <- function(j_result_set) {
  #assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  j_meta_data <- jtry(.jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE))
  verifyNotNull(j_meta_data, "Unable to retrieve JDBC result set meta data for ", j_result_set, " in dbSendQuery")
  j_meta_data
}

close_statement <- function(j_statement) {
  #assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "V", "close"))
}

close_result_set <- function(j_result_set) {
  #assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  jtry(.jcall(j_result_set, "V", "close"))
}
