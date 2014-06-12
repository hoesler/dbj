#' @include JDBCSQLTypes.R
NULL

#' Create a converion definition for a set of SQL types between working and transfer type. 
#' 
#' @param sql_types a vector of sql types as defined in \code{\link{JDBC_SQL_TYPES}}
#' @param convert_from a function which can be applied to a vector and returns a vector
#'   with the values converted to a desired working type.
#' @param convert_to a function which can be applied to a vector of a working type and returns a vector
#'   with values matching to transfer type of the given sql type.
#' @param constraint a function which accepts an object argument and returns a logical indicating if this mapping is applicable for the object.
#' @param create_type a character which provides the default SQL Type used for creating database columns for objects which satisfy the constraint.
#' 
#' @export
rjdbc_mapping <- function(sql_types, convert_from, convert_to, constraint, create_type) {
  assert_that(is.numeric(sql_types) && sql_types %in% JDBC_SQL_TYPES)
  assert_that(is.function(convert_from))
  assert_that(is.function(convert_to))

  structure(list(
    sql_types = sql_types,
    convert_from = convert_from,
    convert_to = convert_to,
    constraint = constraint,
    create_type = create_type
  ), class = "RJDBCMapping")
}

#' @export
#' @rdname rjdbc_mapping
default_rjdbc_mapping <- list(  
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(TINYINT, SMALLINT, INTEGER)),
    identity,
    identity,
    is.integer,
    "INTEGER"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL, BIGINT)),
    identity,
    identity,
    is.numeric,
    "DOUBLE PRECISION"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(BIT, BOOLEAN)),
    identity,
    identity,
    is.logical,
    "BOOLEAN"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(TIME)),
    as.numeric,
    as.numeric,
    is.numeric,
    "TIME"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(DATE)),
    function(x) as.Date(x / 1000 / 60 / 60 / 24, origin = "1970-01-01", tz = "GMT"),
    function(x) as.numeric(x) * 24 * 60 * 60 * 1000, # days to milliseconds
    function(x) is(x, "Date"),
    "DATE"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(TIMESTAMP)),
    function(x) as.POSIXct(x / 1000, origin = "1970-01-01", tz = "GMT"),
    function(x) as.numeric(x) * 1000, # seconds to milliseconds
    function(x) is(x, "POSIXt"),
    "TIMESTAMP"
  ),
  rjdbc_mapping(
    with(JDBC_SQL_TYPES, c(CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR)),
    as.character,
    as.character,
    function(x) is.character(x) || is.factor(x),
    "VARCHAR(255)"
  )
)

#' Convert from transfer type to client type
#' 
#' @param a list of RJDBCMapping mapping definitions
#' @param data the data object to convert
#' @param sql_type the source sql type of the data
convert_from <- function(mapping, data, sql_type) {
  assert_that(is.list(mapping) && all(sapply(mapping, class) == "RJDBCMapping"))
  assert_that(is.numeric(sql_type) && length(sql_type) == 1)

  for (i in seq(length(mapping))) {
    if (sql_type %in% mapping[[i]]$sql_types) {
      return(mapping[[i]]$convert_from(data))
    }
  }

  stop("No conversion rule for sql type '", sql_type, "' was defined") 
}

#' Convert from client type to transfer type
#' 
#' @param a list of RJDBCMapping mapping definitions
#' @param data the data object to convert
#' @param sql_type the target sql type of the data
convert_to <- function(mapping, data, sql_type) {
  assert_that(is.list(mapping))
  assert_that(all(sapply(mapping, class) == "RJDBCMapping"))
  assert_that(is.numeric(sql_type) && length(sql_type) == 1)

  for (i in seq(length(mapping))) {
    if (sql_type %in% mapping[[i]]$sql_types && mapping[[i]]$constraint(data)) {
      return(mapping[[i]]$convert_to(data))
    }
  }

  stop("No conversion rule for sql type '", sql_type, "' and data type ", class(data), " was defined") 
}