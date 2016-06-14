#' @include java_sql_types.R
#' @include r_utils.R
NULL

#' Create a conversion rule
#' 
#' Conversion rules define how to convert data between SQL and R values.
#' 
#' Type mapping in dbj has four data type units: The R working type (RWT), The
#' R transfer type (RTT), the Java transfer type (JTT) and the SQL storage Type
#' (SST). The RWT is the type of a data.frame column you work with on the front
#' end. For writing to or reading from a database, each RWT must be mapped to a
#' SST. For performance reasons of the R to Java communication, conversions
#' between these types must go through an associated RTT/JTT transfer type pair. A JTT is
#' one of the Java raw types (boolean, byte, int, long, float, double) or
#' String. An RTT is the R equivalent of the JTT.
#' In summary, data transformation involves three conversions steps: RWT <-> RTT <-> JTT <-> SST.
#' These steps are defined by the conersion rules.
#' 
#' @param condition A function which accepts the data and additional data attributes and returns a logical
#'   indicating if it is able to convert the data.
#' @param target_type A character vector of length one. For a read_conversion_rule this is an R class,
#'   for a write_conversion_rule, it is an SQL type name.
#' @param conversion A function which accepts data of a transfer type
#'  and transforms into data of type \code{target_type}.
#' 
#' @examples
#' # Convert TIME data, fetched from JDBC as numeric milliseconds, to difftime.
#' read_conversion_rule(
#'   function(jdbc.type, ...) with(JDBC_SQL_TYPES,
#'     jdbc.type == TIME),
#'   function(data, ...) as.difftime(data / 1000, units = "secs"),
#'   function(...) "difftime"
#' )
#' 
#' #' # Convert difftime vectors into numeric vectors of milliseconds and create 
#' write_conversion_rule(
#'   function(data, ...) is.difftime(data),
#'   function(data, ...) as.numeric(data, units = "secs") * 1000,
#'   function(...) "TIME"
#' )
#' @name conversion_rules
NULL

#' @rdname conversion_rules
#' @aliases read_conversion_rule
#' @export
read_conversion_rule <- function(condition, conversion, target_type) {
  assert_that(is.function(condition))
  assert_that(is.function(conversion))
  assert_that(is.function(target_type))

  structure(list(
    condition = condition,
    conversion = conversion,
    target_type = target_type
  ), class = "read_conversion_rule")
}

#' @rdname conversion_rules
#' @aliases write_conversion_rule
#' @export
write_conversion_rule <- function(condition, conversion, target_type) {
  assert_that(is.function(condition))
  assert_that(is.function(conversion))
  assert_that(is.function(target_type))

  structure(list(
    condition = condition,
    conversion = conversion,
    target_type = target_type
  ), class = "write_conversion_rule")
}

find_conversion_rule <- function(conversion_rules, data, data_attributes) {
  for (i in seq_along(conversion_rules)) {
    conversion_rule <- conversion_rules[[i]]
    args <- c(list(data = data), as.list(data_attributes))
    
    conversion_rule_applicable <- do.call(conversion_rule$condition, args)
    if (conversion_rule_applicable) {
      return(conversion_rule)
    }
  }
  stop(sprintf("No conversion rule found in %d rules for data %s and attributes %s",
    length(conversion_rules), deparse(substitute(data)), deparse(substitute(data_attributes))))
}

#' Convert from transfer type to client type
#' 
#' @param read_conversion_rules a list of \code{\link{read_conversion_rule}} objects
#' @param data the data vector to convert
#' @param data_attributes a named list of attributes of data
#' @keywords internal
convert_from_transfer <- function(read_conversion_rules, data, data_attributes) {
  assert_that(is.list(read_conversion_rules))
  assert_that(all(sapply(read_conversion_rules, class) == "read_conversion_rule"))
  
  conversion_rule <- find_conversion_rule(read_conversion_rules, data, data_attributes)

  args <- c(list(data = data), as.list(data_attributes))
  do.call(conversion_rule$conversion, args)
}

#' Convert from client type to transfer type
#' 
#' @param write_conversion_rules a list of \code{\link{write_conversion_rule}} objects
#' @param data the data object to convert
#' @param data_attributes a named list of attributes
#' @keywords internal
convert_to_transfer <- function(write_conversion_rules, data, data_attributes) {
  assert_that(is.list(write_conversion_rules))
  assert_that(all(sapply(write_conversion_rules, class) == "write_conversion_rule"))
  
  conversion_rule <- find_conversion_rule(write_conversion_rules, data, data_attributes)

  args <- c(list(data = data), as.list(data_attributes))
  do.call(conversion_rule$conversion, args)
}

setGeneric("toSQLDataType", function(obj, write_conversions) standardGeneric("toSQLDataType"))

setMethod("toSQLDataType", "data.frame", function(obj, write_conversions) {
  vapply(obj, toSQLDataType, FUN.VALUE = character(1), write_conversions, USE.NAMES = TRUE)
})

setOldClass("AsIs")

setMethod("toSQLDataType", "AsIs", function(obj, write_conversions) {
  toSQLDataType(unclass(obj), write_conversions)
})

setMethod("toSQLDataType", "ANY", function(obj, write_conversions) {
  conversion_rule <- find_conversion_rule(write_conversions, obj, list())
  args <- list(data = obj)   
  db_data_type <- do.call(conversion_rule$target_type, args)
  assert_that(is.character(db_data_type) && length(db_data_type == 1))
  return(db_data_type)
})
