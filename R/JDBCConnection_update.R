#' @include JDBCConnection.R
#' @include JDBCConnection_generics.R
#' @include java_utils.R
#' @include jdbc.R
NULL

#' @describeIn dbSendUpdate Send update query without parameters
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "missing"),
  function(conn, statement, parameters) {
    j_statement <- jdbc_create_prepared_statement(conn, statement)
    on.exit(jdbc_close_statement(j_statement))
    jdbc_execute_update(j_statement)
    invisible(TRUE)
  }
)

#' @describeIn dbSendUpdate Send update query with parameters given as a named list
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "list"),
  function(conn, statement, parameters) {
    assert_that(!is.null(names(parameters)))
    dbSendUpdate(conn, statement, as.data.frame(parameters))
  }
)

#' @describeIn dbSendUpdate Send batch update queries with parameters given as a data.frame
#' @param partition_size the size which will be used to partition the data into separate commits
#' @export
setMethod("dbSendUpdate",  signature(conn = "JDBCConnection", statement = "character", parameters = "data.frame"),
  function(conn, statement, parameters, partition_size = 10000) {
    assert_that(!is.null(names(parameters)))
    assert_that(!any(is.na(names(parameters))))
    assert_that(length(statement) == 1)
    assert_that(nrow(parameters) > 0)

    conversions <- dbGetDriver(conn)@write_conversions

    sapply(partition(parameters, partition_size), function(subset) {
      # Create a new statement for each batch. Reusing a single statement messes up ParameterMetaData (on H2).
      j_statement <- jdbc_create_prepared_statement(conn, statement)
      tryCatch({
        jdbc_batch_update(j_statement, subset, conversions)
      },
      finally = jdbc_close_statement(j_statement))
    })

    invisible(TRUE)
  }
)
