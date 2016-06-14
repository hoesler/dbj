#' @include type_mapping.R
NULL

#' Default type conversion rules
#' @name default-conversion-rules
NULL

#' @export
#' @rdname default-conversion-rules
default_read_conversions <- list(
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == get("NULL")),
    function(data, ...) identity(data),
    function(...) "integer"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(TINYINT, SMALLINT, INTEGER)),
    function(data, ...) identity(data),
    function(...) "integer"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL, BIGINT)),
    function(data, ...) identity(data),
    function(...) "numeric"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(BIT, BOOLEAN)),
    function(data, ...) identity(data),
    function(...) "logical"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR)),
    function(data, ...) identity(data),
    function(...) "character"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == TIME),
    function(data, ...) as.difftime(data / 1000, units = "secs"),
    function(...) "difftime"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == DATE),
    # data is point in time that is time milliseconds after January 1, 1970 00:00:00 GMT
    function(data, ...) structure(as.integer(data / 1000 / 60 / 60 / 24), class = "Date"),
    function(...) "Date"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == TIMESTAMP),
    function(data, ...) as.POSIXct(data / 1000, origin = "1970-01-01", tz = "GMT"),
    function(...) "POSIXct"
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(BINARY, BLOB)),
    function(data, ...) { lapply(data, function(field) { if (all(is.na(field))) NA else as.raw(field) }) },
    function(...) "list"
  )
)

#' @export
#' @rdname default-conversion-rules
default_write_conversions <- list(
  write_conversion_rule(
    function(data, ...) is.integer(data),
    function(data, ...) identity(data),
    function(...) "INTEGER"
  ),
  write_conversion_rule(
    function(data, ...) is.numeric(data),
    function(data, ...) identity(data),
    function(...) "DOUBLE PRECISION"
  ),
  write_conversion_rule(
    function(data, ...) is.logical(data),
    function(data, ...) identity(data),
    function(...) "BOOLEAN"
  ),
  write_conversion_rule(
    function(data, ...) is.difftime(data),
    function(data, ...) as.numeric(data, units = "secs") * 1000,
    function(...) "TIME"
  ),
  write_conversion_rule(
    function(data, ...) is.Date(data),
    # days to milliseconds after January 1, 1970 00:00:00 GMT
    function(data, ...) as.numeric(data) * 24 * 60 * 60 * 1000,
    function(...) "DATE"
  ),
  write_conversion_rule(
    function(data, ...) is.POSIXct(data),
    # seconds to milliseconds after January 1, 1970 00:00:00 GMT
    function(data, ...) as.numeric(data) * 1000,
    function(...) "TIMESTAMP"
  ),
  write_conversion_rule(
    function(data, ...) is.character(data) || is.factor(data),
    function(data, ...) as.character(data),
    function(data, ...) "VARCHAR(255)"
  ),
  write_conversion_rule(
    function(data, ...) is.raw_list(data),
    function(data, ...) identity(data),
    function(...) "BLOB"
  )
)
