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
    function(...) "integer",
    function(data, ...) identity(data)
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(TINYINT, SMALLINT, INTEGER)),
    function(...) "integer",
    function(data, ...) identity(data)
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL, BIGINT)),
    function(...) "numeric",
    function(data, ...) identity(data)
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(BIT, BOOLEAN)),
    function(...) "logical",
    function(data, ...) identity(data)
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR)),
    function(...) "character",
    function(data, ...) identity(data)
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == TIME),
    function(...) "difftime",
    function(data, ...) as.difftime(data / 1000, units = "secs")
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == DATE),
    function(...) "Date",
    # data is point in time that is time milliseconds after January 1, 1970 00:00:00 GMT
    function(data, ...) structure(as.integer(data / 1000 / 60 / 60 / 24), class = "Date")
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type == TIMESTAMP),
    function(...) "POSIXct",
    function(data, ...) as.POSIXct(data / 1000, origin = "1970-01-01", tz = "GMT")
  ),
  read_conversion_rule(
    function(jdbc.type, ...) with(JDBC_SQL_TYPES,
      jdbc.type %in% c(BINARY, BLOB)),
    function(...) "list",
    function(data, ...) { lapply(data, function(field) { if (all(is.na(field))) NA else as.raw(field) }) }
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
    function(data, ...) as.numeric(data),
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
