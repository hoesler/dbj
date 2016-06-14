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
    info = "list",
    read_conversions = "list",
    write_conversions = "list",
    create_new_connection = "function"))

setMethod("initialize", "JDBCDriver", function(.Object, info = list(), ...) {
    .Object <- callNextMethod()
    if (nargs() > 1 && missing(info)) {
      .Object@info <- driver_info()
    }
    .Object
  }
)

#' Legacy JDBCDriver constructor.
#' 
#' This function is present for \code{RJDBC} compatibility. Use \code{\link{driver}} instead. 
#' @param driverClass the Java class name of the JDBC driver to use
#' @param classPath a string of paths separated by the \code{path.sep} variable in \code{\link{.Platform}}
#'                  which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param ... Additional arguments passed to driver()
#' @rdname JDBCDriver-class-legacy
#' @export
#' @keywords internal
JDBC <- function(driverClass = '', classPath = '', ...) {
  driver(driver_class = driverClass, classpath = classPath, ...)
}

create_new_dbj_query_result <- function(j_result_set, conn, statement, ...)
  JDBCQueryResult(j_result_set = j_result_set, connection = conn, statement = statement, ...)
create_new_dbj_update_result <- function(update_count, conn, statement, ...)
  JDBCUpdateResult(update_count = update_count, connection = conn, statement = statement, ...)
create_new_dbj_connection <- function(j_con, drv, ...) {
  JDBCConnection(j_connection = j_con, driver = drv,
    create_new_query_result = create_new_dbj_query_result,
    create_new_update_result = create_new_dbj_update_result, ...)
}

#' Factory function for \code{\linkS4class{JDBCDriver}} objects
#' 
#' Call \code{driver} to create a new \code{\linkS4class{JDBCDriver}}
#' in order to \code{\link[=dbConnect,JDBCDriver-method]{connect}} to databases using the given JDBC driver.
#' 
#' @inheritParams jdbc_register_driver
#' @param dialect The \code{\link{sql_dialect}} to use.
#' @param read_conversions a list of \code{\link[=read_conversion_rule]{read conversions}}.
#' @param write_conversions a list of \code{\link[=write_conversion_rule]{write conversions}}.
#' @param create_new_connection The factory function for JDBCConnection objects.
#' @return A new \code{\linkS4class{JDBCDriver}}
#' @examples
#' library(DBI)
#' library(dbj)
#' 
#' jdbc_register_driver(
#'  c('org.h2.Driver', 'org.apache.derby.jdbc.EmbeddedDriver'),
#'  resolve(
#'    list(
#'      module('com.h2database:h2:1.3.176'),
#'      module('org.apache.derby:derby:10.12.1.1')
#'    ),
#'    repositories = list(maven_local, maven_central)
#'  )
#' )
#' 
#' h2_con <- dbConnect(dbj::driver(), 'jdbc:h2:mem:example_db')
#' derby_con <- dbConnect(dbj::driver(), 'jdbc:derby:memory:example_db;create=true')
#' @export
driver <- function(driver_class = NULL, classpath = NULL,              
                  dialect = NULL,
                  read_conversions = default_read_conversions,
                  write_conversions = default_write_conversions,
                  create_new_connection = create_new_dbj_connection) {
  assert_that(is.null(driver_class) || is.character(driver_class))
  assert_that(is.null(classpath) || is.character(classpath))
  assert_that(is.null(dialect) || is.sql_dialect(dialect))

  if (!missing(driver_class)) {
    jdbc_register_driver(driver_class, classpath)
  }
  
  if (!missing(dialect)) {
    warning("Defining dialects in the driver class is deprecated. Define in dbConnect instead.")
  }

  JDBCDriver(
    read_conversions = read_conversions,
    write_conversions = write_conversions,
    create_new_connection = create_new_connection
  )
}

driver_info <- function() {
  classpath <- paste(rJava::.jclassPath(), collapse = ";")
  dbj_jar_version <- regmatches(classpath, regexec(".*/dbj-(\\d+\\.\\d+\\.\\d+)\\.jar", classpath))[[1]][2]

  list(
    driver.version = utils::packageVersion("dbj"),
    client.version = dbj_jar_version,
    max.connections = NA # TODO: Is there a way to get this information from JDBC?
  )
}

#' @describeIn JDBCDriver JDBC maintains no list of active connections. Returns an empty list.
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
#' @param sql_dialect an \code{\link{sql_dialect}}
#' @family connection functions
#' @export
#' @examples
#' \dontrun{
#' con <- dbConnect(dbj::driver(), 'jdbc:h2:mem:', 'sa', 'sa')
#' }
setMethod("dbConnect", signature(drv = "JDBCDriver"),
  function(drv, url, user = '', password = '', sql_dialect = dialect_for_url(url), ...) {
    j_con <- create_jdbc_connection(url, user, password, ...)
    drv@create_new_connection(j_con, drv, sql_dialect = sql_dialect)
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
})
