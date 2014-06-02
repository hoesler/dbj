#' @include JavaUtils.R
#' @include SQLUtils.R
NULL

#' Set the values of prepared statment.
#' 
#' @param  j_statement a java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
insert_parameters <- function(j_statement, parameter_list) {
  assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  if (length(parameter_list) > 0) {
    for (i in seq(length(parameter_list))) {
      parameter <- parameter_list[[i]]
      if (is.na(parameter)) { # map NAs to NULLs (courtesy of Axel Klenk)
        sql_type <- as.sql_type(parameter)
        jtry(.jcall(j_statement, "V", "setNull", i, as.integer(sql_type), check = FALSE))
      } else if (is.integer(parameter))
        jtry(.jcall(j_statement, "V", "setInt", i, parameter[1], check = FALSE))
      else if (is.numeric(parameter))
        jtry(.jcall(j_statement, "V", "setDouble", i, as.double(parameter)[1], check = FALSE))
      else
        jtry(.jcall(j_statement, "V", "setString", i, as.character(parameter)[1], check = FALSE))
    }
  }
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
  assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "Z", "execute", check = FALSE))
}

execute_update <- function(j_statement) {
  assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "I", "executeUpdate", check = FALSE))
}

add_batch <- function(j_statement) {
  assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(.jcall(j_statement, "V", "addBatch", check = FALSE))
}

#' Execute a batch statement
#' 
#' @param  j_statement a jobjRef object to a java.sql.PreparedStatement object
#' @return integer vector with update counts containing one element for each command in the batch
execute_batch <- function(j_statement) {
  assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  jtry(.jcall(j_statement, "[I", "executeBatch", check = FALSE))
}

get_meta_data <- function(j_result_set) {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")

  j_meta_data <- jtry(.jcall(j_result_set, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE))
  verifyNotNull(j_meta_data, "Unable to retrieve JDBC result set meta data for ", j_result_set, " in dbSendQuery")
  j_meta_data
}

close_statement <- function(j_statement) {
  assert_that(j_statement %instanceof% "java.sql.Statement")
  jtry(.jcall(j_statement, "V", "close"))
}

close_result_set <- function(j_result_set) {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  jtry(.jcall(j_result_set, "V", "close"))
}
