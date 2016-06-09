#' @include java_sql_types.R
NULL

#' Create a read conversion rule
#' 
#' Read conversion rules define how to convert data read from JDBC to R values.
#' 
#' @param condition a function which accepts a list of data attributes and returns a logical
#'  indicating if it is able to convert the data
#' @param r_class the target class of the conversion
#' @param conversion a function which accepts data of a transfer type
#'  and transforms into data of type \code{r_class}.
#' 
#' @export
#' @family conversion functions
#' @examples
#' 
#' # Convert TIME data, fetched from JDBC as numeric milliseconds, to difftime.
#' read_conversion_rule(
#'   function(jdbc.type, ...) with(JDBC_SQL_TYPES,
#'     jdbc.type == TIME),
#'   function(...) "difftime",
#'   function(data, ...) as.difftime(data / 1000, units = "secs")
#' )
read_conversion_rule <- function(condition, r_class, conversion) {
  assert_that(is.function(condition))
  assert_that(is.function(r_class))
  assert_that(is.function(conversion))

  structure(list(
    condition = condition,
    r_class = r_class,
    conversion = conversion
  ), class = "read_conversion_rule")
}

is.raw_list <- function(x) {
  all(vapply(x, function(x) { is.raw(x) || is.na(x) }, logical(1)))
}

is.difftime <- function(x) is(x, "difftime")

is.Date <- function(x) is(x, "Date")

is.POSIXct <- function(x) is(x, "POSIXct")

#' Create a write conversion rule
#' 
#' Write conversion rules define how to convert R values to data which can be written to JDBC.
#' 
#' Group conversion rules as a list and pass them to \code{\link{driver}}.
#' 
#' @param condition a function which accepts the data and additional attributes and returns a logical
#'  indicating if it is able to convert the data
#' @param conversion a function which accepts a data frame column vector and returns it transformed
#'  into a vector of a transfer data type
#' @param create_type a character vector of length 1 which holds a SQL type name used to store data
#'  which satisfies the given condition.
#' 
#' @export
#' @family conversion functions
#' @examples
#' # Convert difftime vectors into numeric vectors of milliseconds and create 
#' write_conversion_rule(
#'   function(data, ...) is.difftime(data),
#'   function(data, ...) as.numeric(data, units = "secs") * 1000,
#'   function(...) "TIME"
#' )
write_conversion_rule <- function(condition, conversion, create_type) {
  assert_that(is.function(condition))
  assert_that(is.function(conversion))
  assert_that(is.function(create_type))

  structure(list(
    condition = condition,
    conversion = conversion,
    create_type = create_type
  ), class = "write_conversion_rule")
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
  
  for (i in seq_along(read_conversion_rules)) {
    conversion_rule <- read_conversion_rules[[i]]
    args <- c(list(data = data), as.list(data_attributes))
    
    conversion_rule_applicable <- do.call(conversion_rule$condition, args)
    if (conversion_rule_applicable) {
      converted_data <- do.call(conversion_rule$conversion, args)
      return(converted_data)
    }
  }

  stop(sprintf("No read conversion rule was defined for attributes %s",
    list(data_attributes))) 
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
  
  for (i in seq_along(write_conversion_rules)) {
    conversion_rule <- write_conversion_rules[[i]]
    args <- c(list(data = data), as.list(data_attributes))
    
    conversion_rule_applicable <- do.call(conversion_rule$condition, args)
    if (conversion_rule_applicable) {
      converted_data <- do.call(conversion_rule$conversion, args)
      return(converted_data)
    }
  }

  stop(sprintf("No write conversion rule was defined for attributes %s",
    list(data_attributes)))
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
  for (i in seq_along(write_conversions)) {
    conversion <- write_conversions[[i]]
    args <- list(data = obj)
    
    if (do.call(conversion$condition, args)) {
      db_data_type <- do.call(conversion$create_type, args)
      assert_that(is.character(db_data_type) && length(db_data_type == 1))
      return(db_data_type)
    }
  }
  stop("No mapping defined for object of type [", paste(class(obj), collapse = ", "), "]")
})
