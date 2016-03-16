#' Create a new SQL dialect environment
#' 
#' @param name The name of the dialect
#' @param sql_create_table A function which generates an SQL statement for creating a table
#' @param sql_append_table A function which generates an SQL statement for adding data to the table
#' @param sql_clear_table A function which generates an SQL statement for truncating a table
#' @param conn An object of type \code{\linkS4class{JDBCConnection}}
#' @param table The table name
#' @param data A data.frame
#' @param temporary If \code{TRUE}, will generate a temporary table statement.
#' @param use_delete If \code{TRUE}, will use DELETE. If \code{FALSE}, TRUNCATE.
#' @return A new structure with class \code{sql_dialect}.
#' @name sql_dialect
NULL

#' @rdname sql_dialect
#' @export
generic_create_table <- function(conn, table, data, temporary = FALSE) {
  assert_that(is(conn, "JDBCConnection"))
  assert_that(is.character(table) && length(table) == 1L)
  assert_that(is.data.frame(data))
  assert_that(is.logical(temporary))

  data_types <- sapply(data, dbDataType, dbObj = conn@driver)
  field_definitions <- paste(dbQuoteIdentifier(conn, names(data)), data_types, collapse = ', ')
  statement <- sprintf(
    "CREATE %s TABLE %s (%s)",
    ifelse(temporary, "TEMPORARY", ""),
    dbQuoteIdentifier(conn, table),
    field_definitions)
  SQL(statement)
}

#' @rdname sql_dialect
#' @export
generic_append_table <- function(conn, table, data) {
  assert_that(is(conn, "JDBCConnection"))
  assert_that(is.character(table) && length(table) == 1L)
  assert_that(is.data.frame(data))

  statement <- sprintf(
    "INSERT INTO %s(%s) VALUES(%s)",
    dbQuoteIdentifier(conn, table),
    paste(dbQuoteIdentifier(conn, names(data)), collapse = ', '),
    paste(rep("?", length(data)), collapse = ', '))
  SQL(statement)
}

#' @rdname sql_dialect
#' @export
generic_clear_table <- function(conn, table, use_delete = FALSE) {
  assert_that(is(conn, "JDBCConnection"))
  assert_that(is.character(table) && length(table) == 1L)
  assert_that(is.logical(use_delete))

  if (use_delete) {
    SQL(sprintf("DELETE FROM %s", dbQuoteIdentifier(conn, table)))
  } else {
    SQL(sprintf("TRUNCATE TABLE %s", dbQuoteIdentifier(conn, table)))
  }
}

#' @rdname sql_dialect
#' @export
sql_dialect <- function(
  name,
  sql_create_table = generic_create_table,
  sql_append_table = generic_append_table,
  sql_clear_table = generic_clear_table) {

  assert_that(is.function(sql_create_table))
  assert_that(is.function(sql_append_table))
  assert_that(is.function(sql_clear_table))

  structure(list(
    name = name,
    env = list(
      sql_create_table = sql_create_table,
      sql_append_table = sql_append_table,
      sql_clear_table = sql_clear_table
    )
  ), class = "sql_dialect")
}

is.sql_dialect <- function(x) inherits(x, "sql_dialect")

#' @rdname sql_dialect
#' @export
generic_sql <- sql_dialect("generic")
