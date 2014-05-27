#' @include JavaUtils.R
#' @include SQLUtils.R
NULL

#' Set the values of prepared statment.
#' 
#' @param  j_statement a java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
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

# Get the corresponding int value (java.sql.Types) for the given object type
as.sqlType <- function(object) {
  if (is.integer(object)) 4 # INTEGER
  else if (is.numeric(object)) 8 # DOUBLE
  else 12 # VARCHAR
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