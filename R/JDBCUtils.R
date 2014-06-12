#' @include JavaUtils.R
#' @include SQLUtils.R
#' @include JDBCSQLTypes.R
#' @include JDBCMapping.R
NULL

#' Set the values of prepared statment.
#' 
#' @param  j_statement a java reference object to a java.sql.PreparedStatement
#' @param  parameter_list a list of parameter values to fill the statement with
insert_parameters <- function(j_statement, parameter_list, mapping) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.list(parameter_list))
  if (length(parameter_list) > 0) {
    j_table <- create_j_table(j_statement, as.data.frame(parameter_list), mapping)

    jtry(.jcall("info/urbanek/Rpackage/RJDBC/PreparedStatements", "V", "insert",
      .jcast(j_statement, "java/sql/PreparedStatement"), .jcast(j_table, "info/urbanek/Rpackage/RJDBC/Table"), as.integer(0), check = FALSE))
  }
  invisible(NULL)
}

create_prepared_statement <- function(conn, statement) {
  ## if the statement starts with {call or {? = call then we use CallableStatement 
  if (any(grepl("^\\{(call|\\? = *call)", statement))) {
    return(prepare_call(conn, statement))
  } else {
    return(prepare_statement(conn, statement))
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

#' Transform a data frame into a java reference to a info/urbanek/Rpackage/RJDBC/Table
#' @param j_statement a jobjRef to a java.sql.PreparedStatement
#' @param data a data.frame
#' @param mapping a list of RJDBCMapping objects
create_j_table <- function(j_statement, data, mapping) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_statement_meta <- jtry(.jcall(j_statement, "Ljava/sql/ParameterMetaData;", "getParameterMetaData", check = FALSE))

  j_columns <- unlist(lapply(seq(ncol(data)), function(column_index) {
    column <- unlist(data[,column_index])
    sql_type <- jtry(.jcall(j_statement_meta, "I", "getParameterType", column_index, check = FALSE))
    is_nullable <- jtry(.jcall(j_statement_meta, "I", "isNullable", column_index, check = FALSE))
    
    column_class <- NULL
    column_data <- NULL

    with(JDBC_SQL_TYPES,
      if (sql_type %in% c(BIT, BOOLEAN)) {
        column_class <<- "info/urbanek/Rpackage/RJDBC/BooleanColumn"
        column_data <<- as.logical(convert_to(mapping, column, sql_type))
      } else if (sql_type %in% c(TINYINT, SMALLINT, INTEGER)) {
        column_class <<- "info/urbanek/Rpackage/RJDBC/IntegerColumn"
        column_data <<- as.integer(convert_to(mapping, column, sql_type))
      } else if (sql_type %in% c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL)) {
        column_class <<- "info/urbanek/Rpackage/RJDBC/DoubleColumn"
        column_data <<- as.numeric(convert_to(mapping, column, sql_type))
      } else if (sql_type %in% c(BIGINT, DATE, TIME, TIMESTAMP)) {
        column_class <<- "info/urbanek/Rpackage/RJDBC/LongColumn"
        column_data <<- .jlong(as.numeric(convert_to(mapping, column, sql_type)))
      } else if (sql_type %in% c(VARCHAR, CHAR, LONGVARCHAR, NVARCHAR, NCHAR, LONGNVARCHAR)) {
        column_class <<- "info/urbanek/Rpackage/RJDBC/StringColumn"
        column_data <<- as.character(convert_to(mapping, column, sql_type))
      } else {
        stop("Unsupported SQL type '", sql_type, "'")
      }
    )

    assert_that(!any(is.null(c(column_class, column_data))))    

    if (is_nullable > 0 && NA %in% column) {
      jtry(.jcall(column_class, sprintf("L%s;", column_class),
        "create", sql_type, .jarray(column_data), .jarray(is.na(column)), check = FALSE))
    } else {
      if (NA %in% column) {
        stop("Parameter ", column_index, " is not nullable but data contains NA")
      }
      jtry(.jcall(column_class, sprintf("L%s;", column_class),
        "create", sql_type, .jarray(column_data), check = FALSE))
    }

  }))

  jtry(.jcall("info/urbanek/Rpackage/RJDBC/ArrayListTable", "Linfo/urbanek/Rpackage/RJDBC/ArrayListTable;",
    "create", .jarray(j_columns, contents.class = "info/urbanek/Rpackage/RJDBC/Column"), check = FALSE))
}

batch_insert <- function(j_statement, data, mapping) {
  #assert_that(j_statement %instanceof% "java.sql.PreparedStatement")
  assert_that(is.data.frame(data))

  j_table <- create_j_table(j_statement, data, mapping)

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
