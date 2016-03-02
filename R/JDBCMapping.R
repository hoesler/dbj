#' @include JDBCSQLTypes.R
NULL

#' Create a JDBCReadConversion object. 
#' 
#' @param condition a function which accepts a list and returns a logical
#' @param r_class the target class of the conversion
#' @param conversion a function which accepts a data frame column vector and returns it transformed into a vector of a transfer data type.
#' 
#' @export
#' @family conversion functions
read_conversion <- function(condition, r_class, conversion) {
  assert_that(is.function(condition))
  assert_that(is.character(r_class))
  assert_that(is.function(conversion))

  structure(list(
    condition = condition,
    r_class = r_class,
    conversion = conversion
  ), class = "JDBCReadConversion")
}

#' @param sql_types a numeric vector of JDBC_SQL_TYPES
#' @export
#' @rdname read_conversion
sqltype_read_conversion <- function(sql_types, r_class, conversion) {
  assert_that(is.numeric(sql_types))
  
  read_conversion(function(attributes) {
    assert_that(is.list(attributes) && c("field.type") %in% names(attributes))
    with(JDBC_SQL_TYPES, JDBC_SQL_TYPES[names(JDBC_SQL_TYPES) == attributes$field.type] %in% sql_types)
  }, r_class, conversion)
}

#' The dafault read conversions
#' @format The default conversions
#' @export
#' @rdname read_conversion
default_read_conversions <- list(
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(TINYINT, SMALLINT, INTEGER)),
    "integer",
    identity
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL, BIGINT)),
    "numeric",
    identity
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(BIT, BOOLEAN)),
    "logical",
    identity
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR)),
    "character",
    identity
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(TIME)),
    "numeric",
    identity
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(DATE)),
    "Date",
    function(data) as.Date(data / 1000 / 60 / 60 / 24, origin = "1970-01-01", tz = "GMT")
  ),
  sqltype_read_conversion(
    with(JDBC_SQL_TYPES, c(TIMESTAMP)),
    "POSIXct",
    function(data) as.POSIXct(data / 1000, origin = "1970-01-01", tz = "GMT")
  )
)

#' Create a JDBCWriteConversion object. 
#' 
#' @param condition a function which accepts a list and returns a logical.
#' @param conversion a function which accepts a data frame column vector and returns it transformed into a vector of a transfer data type.
#' @param create_type a character vector of length 1 which holds a sql type name used to store data which sattisfies the given condition.
#' 
#' @export
#' @family conversion functions
write_conversion <- function(condition, conversion, create_type) {
  assert_that(is.function(condition))
  assert_that(is.function(conversion))
  assert_that(is.character(create_type))

  structure(list(
    condition = condition,
    conversion = conversion,
    create_type = create_type
  ), class = "JDBCWriteConversion")
}

#' @param class_names a character vector of class names
#' @export
#' @rdname write_conversion
mapped_write_conversion <- function(class_names, conversion, create_type) {
  assert_that(is.character(class_names))
 
  write_conversion(function(attributes) {
    assert_that(is.list(attributes) && all(c("class_names") %in% names(attributes)))
    any(attributes$class_names %in% class_names)
  }, conversion, create_type)
}

#' @format The default conversions
#' @export
#' @rdname write_conversion
default_write_conversions <- list(
  mapped_write_conversion(
    c("integer"),
    identity,
    "INTEGER"
  ),
  mapped_write_conversion(
    "numeric",
    identity,
    "DOUBLE PRECISION"
  ),
  mapped_write_conversion(
    "logical",
    as.numeric,
    "BOOLEAN"
  ),
  mapped_write_conversion(
    "numeric",
    as.numeric,
    "TIME"
  ),
  mapped_write_conversion(
    "Date",
    function(data) as.numeric(data) * 24 * 60 * 60 * 1000, # days to milliseconds
    "DATE"
  ),
  mapped_write_conversion(
    "POSIXt",
    function(data) as.numeric(data) * 1000, # seconds to milliseconds
    "TIMESTAMP"
  ),
  mapped_write_conversion(
    c("character", "factor"),
    as.character,
    "VARCHAR(255)"
  )
)

#' Convert from transfer type to client type.
#' 
#' @param conversions a list of JDBCReadConversion objects
#' @param data the data vector to convert
#' @param data_attributes a named list of attributes of data
#' @keywords internal
convert_from <- function(conversions, data, data_attributes) {
  assert_that(is.list(conversions) && all(sapply(conversions, class) == "JDBCReadConversion"))
  assert_that(is.list(data_attributes) && "field.type" %in% names(data_attributes))

  for (i in seq_along(conversions)) {
    if (conversions[[i]]$condition(data_attributes)) {
      return(conversions[[i]]$conversion(data))
    }
  }

  stop(sprintf("No read conversion rule was defined for attributes %s",
    list(data_attributes))) 
}

#' Convert from client type to transfer type
#' 
#' @param conversions a list of JDBCWriteConversion objects
#' @param data the data object to convert
#' @param data_attributes a named list of attributes
#' @keywords internal
convert_to <- function(conversions, data, data_attributes) {
  assert_that(is.list(conversions))
  assert_that(all(sapply(conversions, class) == "JDBCWriteConversion"))
  assert_that(is.list(data_attributes) && "class_names" %in% names(data_attributes))

  for (i in seq_along(conversions)) {
    if (conversions[[i]]$condition(data_attributes)) {
      return(conversions[[i]]$conversion(data))
    }
  }

  stop(sprintf("No write conversion rule was defined for attributes %s",
    list(data_attributes)))
}
