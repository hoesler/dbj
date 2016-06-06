#' Create a new SQL dialect environment
#' 
#' @param name The name of the dialect
#' @param sql_create_table A function which generates an SQL statement for creating a table
#' @param sql_append_table A function which generates an SQL statement for adding data to the table
#' @param sql_clear_table A function which generates an SQL statement for truncating a table
#' @param sql_quote_identifier The function called by the \code{dbQuoteIdentifier,JDBCConnection-character-method} method
#' @param sql_quote_string The function called by the \code{dbQuoteString,JDBCConnection-character-method} method
#' @param sql_remove_table The function called by the \code{dbRemoveTable,JDBCConnection-character-method} method
#' @param conn,con An object of type \code{\linkS4class{JDBCConnection}}
#' @param table The table name
#' @param fields Either a character vector or a data frame.
#' @param values A data.frame
#' @param temporary If \code{TRUE}, will generate a temporary table statement.
#' @param use_delete If \code{TRUE}, will use DELETE. If \code{FALSE}, TRUNCATE.
#' @param driver_class The full classname of a Java Driver class.
#' @inheritParams DBI::rownames
#' @param x A character vector to label as being escaped SQL.
#' @param ... Other parameters passed on to methods.
#' @return A new structure with class \code{sql_dialect}.
#' @name sql_dialect
NULL

#' A generator function for a generic CREATE TABLE statment
#' @param table_name The quoted table name
#' @param field_names A vector of quoted field names
#' @param field_types A vector of the field types
#' @rdname sql_dialect
#' @export
generic_create_statement_generator <- function(table_name, field_names, field_types, temporary) {
  paste0(
    "CREATE ", if (temporary) "TEMPORARY ", "TABLE ", table_name, " (\n",
    "  ", paste(paste0(field_names, " ", field_types), collapse = ",\n  "), "\n)\n"
  )
}

#' @rdname sql_dialect
#' @param statement_generator A function which creates an sql create table statement  
#' @export
create_table_template <- function(statement_generator = generic_create_statement_generator) {
  function(con, table, fields, row.names = NA, temporary = FALSE, ...) {
    table <- dbQuoteIdentifier(con, table)

    if (is.data.frame(fields)) {
      fields <- sqlRownamesToColumn(fields, row.names)
      fields <- vapply(fields, function(x) dbDataType(con, x), character(1))
    }

    field_names <- dbQuoteIdentifier(con, names(fields))
    field_types <- unname(fields)

    statement <- statement_generator(table, field_names, field_types, temporary)
    SQL(statement)
  }
}

#' @rdname sql_dialect
#' @export
generic_create_table <- create_table_template()

#' @rdname sql_dialect
#' @export
generic_append_table <- function (con, table, values, row.names = NA, ...) 
{
    table <- dbQuoteIdentifier(con, table)
    values <- sqlRownamesToColumn(values[0, , drop = FALSE], 
        row.names)
    fields <- dbQuoteIdentifier(con, names(values))
    SQL(paste0("INSERT INTO ", table, "\n", "  (", paste(fields, 
        collapse = ", "), ")\n", "VALUES\n", paste0("  (", paste0(rep('?', length(fields)),
        collapse = ", "), ")", collapse = ",\n")))
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

generic_remove_table <- function(conn, name) paste("DROP TABLE", dbQuoteIdentifier(conn, name))

quote_identifier_template <- function(quote_character = "\"") {
  function(conn, x, ...) {
    x <- gsub(quote_character, paste0(quote_character, quote_character), x, fixed = TRUE)
    SQL(paste(quote_character, encodeString(x), quote_character, sep = ""))
  }
}

#' @rdname sql_dialect
#' @export
generic_quote_identifier <- getMethod("dbQuoteIdentifier",
  signature = c("DBIConnection", "character"),
  where = asNamespace('DBI'))@.Data

#' @rdname sql_dialect
#' @export
generic_quote_string <- getMethod("dbQuoteString",
  signature = c("DBIConnection", "character"),
  where = asNamespace('DBI'))@.Data

#' @rdname sql_dialect
#' @export
sql_dialect <- function(
  name,
  sql_create_table = generic_create_table,
  sql_append_table = generic_append_table,
  sql_clear_table = generic_clear_table,
  sql_remove_table = generic_remove_table,
  sql_quote_identifier = generic_quote_identifier,
  sql_quote_string = generic_quote_string) {

  assert_that(is.function(sql_create_table))
  assert_that(is.function(sql_append_table))
  assert_that(is.function(sql_clear_table))
  assert_that(is.function(sql_quote_identifier))
  assert_that(is.function(sql_quote_string))

  structure(
    list(
      sql_create_table = sql_create_table,
      sql_append_table = sql_append_table,
      sql_clear_table = sql_clear_table,
      sql_remove_table = sql_remove_table,
      sql_quote_identifier = sql_quote_identifier,
      sql_quote_string = sql_quote_string
    ),
    class = "sql_dialect",
    name = name
  )
}

is.sql_dialect <- function(x) inherits(x, "sql_dialect")

#' @rdname sql_dialect
#' @export
generic_sql <- sql_dialect("generic")

# maps diver classes to a list of arguments to sql_dialect()
known_sql_dialects = list(
  'org.h2.Driver' = list(
    name = 'H2',
    sql_create_table = create_table_template(
      function(table_name, field_names, field_types, temporary) {
        paste0(
          "CREATE ", if (temporary) "LOCAL TEMPORARY ", "TABLE ", table_name, " (\n",
          "  ", paste(paste0(field_names, " ", field_types), collapse = ",\n  "), "\n)\n"
        )
      })
  ),
  'org.apache.derby.jdbc.EmbeddedDriver' = list(
    name = 'Derby',
    sql_create_table = create_table_template(
      function(table_name, field_names, field_types, temporary) {
        if (temporary) {
          paste0("DECLARE GLOBAL TEMPORARY TABLE ", table_name, " (\n",
            "  ", paste(paste0(field_names, " ", field_types), collapse = ",\n  "), "\n) NOT LOGGED\n"
          )
        } else {
          paste0(
            "CREATE TABLE ", table_name, " (\n",
            "  ", paste(paste0(field_names, " ", field_types), collapse = ",\n  "), "\n)\n"
          )
        }
      }),
    sql_append_table = function(con, table, values, row.names = NA, temporary = FALSE, ...) {
        table <- dbQuoteIdentifier(con, table)
        values <- sqlRownamesToColumn(values[0, , drop = FALSE], 
            row.names)
        fields <- dbQuoteIdentifier(con, names(values))
        SQL(paste0("INSERT INTO ", if (temporary) "SESSION.", table, "\n", "  (", paste(fields, 
            collapse = ", "), ")\n", "VALUES\n", paste0("  (", paste0(rep('?', length(fields)),
            collapse = ", "), ")", collapse = ",\n")))
    }
  ),
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
