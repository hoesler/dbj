#' @include JDBCObject.R
#' @include java_utils.R
#' @include java_jdbc_utils.R
#' @include type_mapping.R
#' @include sql_dialect.R
NULL

#' JDBCDriver class
#'
#' \code{JDBCDriver} objects are usually created by 
#' \code{\link[dbj]{driver}}.
#' @keywords internal
#' @export
JDBCDriver <- setClass("JDBCDriver",
  contains = c("DBIDriver", "JDBCObject"),
  slots = c(
    driverClass = "character",
    j_drv = "jobjRef",
    info = "list",
    read_conversions = "list",
    write_conversions = "list",
    dialect = "ANY",
    create_new_connection = "function"))

setMethod("initialize", "JDBCDriver", function(.Object, j_drv, dialect, driverClass, ...) {
    .Object <- callNextMethod()
    if (!missing(j_drv)) {
      .Object@info <- driver_info(j_drv, dialect, driverClass)
    }
    .Object
  }
)

#' Legacy JDBCDriver constructor.
#' 
#' This function is present for \code{RJDBC} compatibility. Use \code{\link{driver}} instead. 
#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by the \code{path.sep} variable in \code{\link{.Platform}}
#'                  which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param ... Additional arguments passed to driver()
#' @rdname JDBCDriver-class-legacy
#' @export
#' @keywords internal
JDBC <- function(driverClass = '', classPath = '', ...) {  
  driver(driverClass, classPath, ...)
}

create_new_dbj_query_result <- function(j_result_set, conn, statement)
  JDBCQueryResult(j_result_set = j_result_set, connection = conn, statement = statement)
create_new_dbj_update_result <- function(update_count, conn, statement)
  JDBCUpdateResult(update_count = update_count, connection = conn, statement = statement)
create_new_dbj_connection <- function(j_con, drv) {
  JDBCConnection(j_connection = j_con, driver = drv,
    create_new_query_result = create_new_dbj_query_result,
    create_new_update_result = create_new_dbj_update_result)
}

#' Factory function for \code{\linkS4class{JDBCDriver}} objects
#' 
#' Call \code{driver} to create a new \code{\linkS4class{JDBCDriver}}
#' in order to \code{\link[=dbConnect,JDBCDriver-method]{connect}} to databases using the given JDBC driver.
#' 
#' @inheritParams create_jdbc_driver
#' @param read_conversions a list of \code{\link[=read_conversion]{read conversions}}.
#' @param write_conversions a list of \code{\link[=write_conversion]{write conversions}}.
#' @param dialect The \code{\link{sql_dialect}} to use.
#' @param create_new_connection The factory function for JDBCConnection objects.
#'  Should only be modified by packages extending JDBCDriver and related classes. 
#' @return A new \code{\linkS4class{JDBCDriver}}
#' @examples
#' \dontrun{
#' drv <- dbj::driver('org.h2.Driver', '~/h2.jar')
#' con1 <- dbConnect(drv, 'jdbc:h2:mem:')
#' con2 <- dbConnect(drv, 'jdbc:h2:file:~/foo')
#' }
#' @export
driver <- function(driverClass, classPath = '',
                  read_conversions = default_read_conversions,
                  write_conversions = default_write_conversions,
                  dialect = guess_dialect(driverClass),
                  create_new_connection = create_new_dbj_connection) {
  assert_that(is.character(driverClass))
  assert_that(is.character(classPath))
  assert_that(is.sql_dialect(dialect))

  j_drv = create_jdbc_driver(driverClass, classPath)

  JDBCDriver(
    driverClass = driverClass,
    j_drv = j_drv,
    read_conversions = read_conversions,
    write_conversions = write_conversions,
    dialect = dialect,
    create_new_connection = create_new_connection
  )
}

driver_info <- function(j_drv, dialect, driverClass) {
  list(
    driver.version = utils::packageVersion("dbj"),
    client.version = paste(
      jdbc_driver_major_version(j_drv),
      jdbc_driver_minor_version(j_drv),
      sep = "."),
    max.connections = NA, # TODO: Is there a way to get this information from JDBC?
    driver.class = driverClass,
    sql.dialect = dialect$name  
  )
}

#' @describeIn JDBCDriver JDBC maintains no list of acitve connections. Returns an empty list.
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored.
#' @family connection functions
#' @export
setMethod("dbListConnections", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    list()
  }
)

#' @describeIn JDBCDriver Unloading a \code{JDBCDriver} has no effect. Returns \code{TRUE}.
#' @export
setMethod("dbUnloadDriver", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    invisible(TRUE)
  }
)

#' Connect to a database
#' 
#' Connect to a database using a
#' \href{https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url}{JDBC URL}.
#'
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @inheritParams create_jdbc_connection
#' @family connection functions
#' @export
#' @examples
#' \dontrun{
#' drv <- dbj::driver('org.h2.Driver', '~/h2.jar')
#' con <- dbConnect(drv, 'jdbc:h2:mem:', 'sa', 'sa')
#' }
setMethod("dbConnect", signature(drv = "JDBCDriver"),
  function(drv, url, user = '', password = '', ...) {
    j_con <- create_jdbc_connection(drv@j_drv, url, user, password, ...)
    drv@create_new_connection(j_con, drv)
  }
)

#' @describeIn JDBCDriver Returns a list with \code{driver.version}, \code{client.version} and \code{max.connections}.
#' @param dbObj An object of class \code{\linkS4class{JDBCDriver}}
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    dbObj@info
  }
)

#' @describeIn JDBCDriver returns the JDBCDriver object.
#' @export
#' @keywords internal
setMethod("dbGetDriver", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    dbObj
  }
)

#' @describeIn JDBCDriver returns the DB data type as defined in the \code{write_conversions}.
#' @param obj An R object whose SQL type we want to determine.
#' @export
setMethod("dbDataType", signature(dbObj = "JDBCDriver"),
  function(dbObj, obj, ...) {
    toSQLDataType(obj, dbObj@write_conversions)
  }
)

#' @describeIn JDBCDriver always \code{TRUE}.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    TRUE
  }
)

#' @describeIn JDBCDriver Prints a short info about the driver.
#' @param object An object of class \code{\linkS4class{JDBCDriver}}
#' @export
setMethod("show", "JDBCDriver", function(object) {
  cat("<JDBCDriver>\n")
  if (dbIsValid(object)) {
    cat("  Driver Class: ", object@driverClass, "\n", sep = "")
  }
})
