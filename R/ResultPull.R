#' @include JavaUtils.R
NULL

create_result_pull <- function(j_result_set) {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  
  rp <- .jnew("info/urbanek/Rpackage/RJDBC/JDBCResultPull", .jcast(j_result_set, "java/sql/ResultSet"), check = FALSE)
  verifyNotNull(rp, "Failed to instantiate JDBCResultPull hepler object")
  rp
}

fetch_resultpull <- function(j_result_pull, rows, column_info) {
  java_table <- jtry(.jcall(j_result_pull, "Linfo/urbanek/Rpackage/RJDBC/Table;", "fetch", as.integer(rows), check = FALSE))
  verifyNotNull(java_table, "Table creation failed")
  column_count <- jtry(.jcall(java_table, "I", "columnCount", check = FALSE))
  row_count <- jtry(.jcall(java_table, "I", "rowCount", check = FALSE))

  column_list <- lapply(seq(column_count), function(column_index) {
    column <- jtry(.jcall(java_table, "Linfo/urbanek/Rpackage/RJDBC/Column;", "getColumn", as.integer(column_index - 1), check = FALSE))

    column_data <- c()
    if (column_info[column_index, "type"] == "numeric") {
      column_data <- jtry(.jcall(column, "[D", "toDoubleArray", check = FALSE))
    } else {
      column_data <- jtry(.jcall(column, "[Ljava/lang/String;", "toStringArray", check = FALSE))     
    }

    column_data
  })

  # as.data.frame is expensive - create it on the fly from the list
  attr(column_list, "row.names") <- c(NA_integer_, row_count)
  class(column_list) <- "data.frame"
  names(column_list) <- column_info$label
  column_list
}