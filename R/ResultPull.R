#' @include JavaUtils.R
#' @include JDBCMapping.R
NULL

create_result_pull <- function(j_result_set) {
  assert_that(j_result_set %instanceof% "java.sql.ResultSet")
  .jnew("com/github/hoesler/dbj/JDBCResultPull", .jcast(j_result_set, "java/sql/ResultSet"), check = TRUE)
}

fetch_resultpull <- function(j_result_pull, rows, column_info, read_conversions, fetch_size) {
  assert_that(is.data.frame(column_info))
  assert_that(all(c("nullable", "label") %in% names(column_info)))
  java_table <- jtry(.jcall(j_result_pull, "Lcom/github/hoesler/dbj/Table;", "fetch", as.integer(rows), as.integer(fetch_size), check = FALSE))
  verifyNotNull(java_table, "Table creation failed")
  
  column_count <- jtry(.jcall(java_table, "I", "columnCount", check = FALSE))
  if (column_count == 0) {
    return(data.frame())
  } else {
    assert_that(nrow(column_info) == column_count)
  }

  row_count <- jtry(.jcall(java_table, "I", "rowCount", check = FALSE))

  column_list <- lapply(seq(column_count), function(column_index) {
    j_column <- jtry(.jcall(java_table, "Lcom/github/hoesler/dbj/Column;", "getColumn", as.integer(column_index - 1), check = FALSE))
    column_class_name <- jtry(.jcall(j_column, "S", "getSimpleClassName", check = FALSE))

    column_data <- c()
    if (column_class_name == "UnsupportedTypeColumn") {
      column_data <- rep(NA, row_count)
    } else if (column_class_name == "NullColumn") {
      column_data <- rep(NA, row_count)
    } else if (column_class_name == "BooleanColumn") {
      column_data <- jtry(.jcall(j_column, "[Z", "toBooleans", check = FALSE))
    } else if (column_class_name == "IntegerColumn") {
      column_data <- jtry(.jcall(j_column, "[I", "toInts", check = FALSE))
    } else if (column_class_name == "LongColumn") {
      column_data <- jtry(.jcall(j_column, "[J", "toLongs", check = FALSE))
    } else if (column_class_name == "DoubleColumn") {
      column_data <- jtry(.jcall(j_column, "[D", "toDoubles", check = FALSE))
    } else if (column_class_name == "StringColumn"){
      column_data <- jtry(.jcall(j_column, "[S", "toStrings", check = FALSE))    
    } else {
      stop("Unexpeted type")
    }

    # set NA values
    if (column_info[column_index, "nullable"] > 0 && column_class_name != "UnsupportedTypeColumn") {
      na <- jtry(.jcall(j_column, "[Z", "getNA", check = FALSE))
      if (length(na) != length(column_data)) {
        stop("NA length mismatch")
      }
      column_data <- replace(column_data, na, NA)
    }

    # convert column data
    column_data <- convert_from(read_conversions, column_data, as.list(column_info[column_index,]))

    column_data
  })

  # as.data.frame is expensive - create it on the fly from the list
  attr(column_list, "row.names") <- c(NA_integer_, row_count)
  class(column_list) <- "data.frame"
  names(column_list) <- column_info$label
  column_list
}