#' Create a new SQL dialect environment
#' 
#' @param name The name of the dialect
#' @param sql_create_table A function which generates an SQL statement for creating a table
#' @param sql_append_table A function which generates an SQL statement for adding data to the table
#' @param sql_clear_table A function which generates an SQL statement for truncating a table
#' @param sql_quote_identifier The function called by the \code{dbQuoteIdentifier,JDBCConnection-character-method} method
#' @param sql_quote_string The function called by the \code{dbQuoteString,JDBCConnection-character-method} method
#' @param conn An object of type \code{\linkS4class{JDBCConnection}}
#' @param table The table name
#' @param data A data.frame
#' @param temporary If \code{TRUE}, will generate a temporary table statement.
#' @param use_delete If \code{TRUE}, will use DELETE. If \code{FALSE}, TRUNCATE.
#' @param driver_class The full classname of a Java Driver class.
#' @param x A character vector to label as being escaped SQL.
#' @param ... Other parameters passed on to methods.
#' @return A new structure with class \code{sql_dialect}.
#' @name sql_dialect
NULL

create_table_template <- function(temporary_statement = "TEMPORARY") {
  function(conn, table, data, temporary = FALSE, ...) {
    assert_that(is(conn, "JDBCConnection"))
    assert_that(is.character(table) && length(table) == 1L)
    assert_that(is.data.frame(data))
    assert_that(is.logical(temporary))

    data_types <- sapply(data, dbDataType, dbObj = conn@driver)
    field_definitions <- paste(dbQuoteIdentifier(conn, names(data)), data_types, collapse = ', ')
    statement <- sprintf(
      "CREATE %s TABLE %s (%s)",
      ifelse(temporary, temporary_statement, ""),
      dbQuoteIdentifier(conn, table),
      field_definitions)
    SQL(statement)
  }
}

#' @rdname sql_dialect
#' @export
generic_create_table <- create_table_template()

#' @rdname sql_dialect
#' @export
generic_append_table <- function(conn, table, data, ...) {
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

quote_identifier_template <- function(quote_character = "\"") {
  function(conn, x, ...) {
    x <- gsub(quote_character, paste0(quote_character, quote_character), x, fixed = TRUE)
    SQL(paste(quote_character, encodeString(x), quote_character, sep = ""))
  }
}

#' @rdname sql_dialect
#' @export
generic_quote_identifier <- getMethod("dbQuoteIdentifier", signature = c("DBIConnection", "character"), where = asNamespace('DBI'))@.Data

#' @rdname sql_dialect
#' @export
generic_quote_string <- getMethod("dbQuoteString", signature = c("DBIConnection", "character"), where = asNamespace('DBI'))@.Data

#' @rdname sql_dialect
#' @export
sql_dialect <- function(
  name,
  sql_create_table = generic_create_table,
  sql_append_table = generic_append_table,
  sql_clear_table = generic_clear_table,
  sql_quote_identifier = generic_quote_identifier,
  sql_quote_string = generic_quote_string) {

  assert_that(is.function(sql_create_table))
  assert_that(is.function(sql_append_table))
  assert_that(is.function(sql_clear_table))
  assert_that(is.function(sql_quote_identifier))
  assert_that(is.function(sql_quote_string))

  structure(list(
    name = name,
    env = list(
      sql_create_table = sql_create_table,
      sql_append_table = sql_append_table,
      sql_clear_table = sql_clear_table,
      sql_quote_identifier = sql_quote_identifier,
      sql_quote_string = sql_quote_string
    )
  ), class = "sql_dialect")
}

is.sql_dialect <- function(x) inherits(x, "sql_dialect")

#' @rdname sql_dialect
#' @export
generic_sql <- sql_dialect("generic")

# maps diver classes to a list of arguments to sql_dialect()
known_sql_dialects = list(
  'org.h2.Driver' = list(name = 'H2', sql_create_table = create_table_template("LOCAL TEMPORARY")),
  'com.mysql.jdbc.Driver' = list(name = 'MySQL', sql_quote_identifier = quote_identifier_template('`'))
)

#' @rdname sql_dialect
#' @export
guess_dialect <- function(driver_class) {
  dialect <-
  if (driver_class %in% names(known_sql_dialects)) {
    do.call(sql_dialect, known_sql_dialects[[driver_class]])
  } else {
    generic_sql
  }

  return(dialect)  
}
