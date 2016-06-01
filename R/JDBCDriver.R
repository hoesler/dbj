#' @include JDBCObject.R
#' @include JavaUtils.R
#' @include JDBCUtils.R
#' @include JDBCMapping.R
#' @include SQLDialect.R
NULL

#' Class JDBCDriver with factory methods.
#'
#' \code{JDBCDriver} objects are usually created by 
#' \code{\link[dbj]{driver}}.
#' @export
setClass("JDBCDriver",
  contains = c("DBIDriver", "JDBCObject"),
  slots = c(
    driverClass = "character",
    j_drv = "jobjRef",
    info = "list",
    read_conversions = "list",
    write_conversions = "list",
    dialect = "ANY"))

#' Legacy JDBCDriver constructor.
#' 
#' This function is present for \code{RJDBC} compatibility. Use \code{\link{driver}} instead. 
#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by the \code{path.sep} variable in \code{\link{.Platform}} which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param ... Additional arguments passed to driver()
#' @rdname JDBCDriver-class-legacy
#' @export
#' @keywords internal
JDBC <- function(driverClass = '', classPath = '', ...) {  
  driver(driverClass, classPath, ...)
}

#' @inheritParams create_jdbc_driver
#' @param read_conversions a list of JDBCReadConversion objects.
#' @param write_conversions a list of JDBCWriteConversion objects.
#' @param dialect The \code{\link{sql_dialect}} to use.
#' @rdname JDBCDriver-class
#' @export
driver <- function(driverClass, classPath = '',
                  read_conversions = default_read_conversions,
                  write_conversions = default_write_conversions,
                  dialect = guess_dialect(driverClass)) {
  assert_that(is.character(driverClass))
  assert_that(is.character(classPath))
  assert_that(is.sql_dialect(dialect))

  j_drv = create_jdbc_driver(driverClass, classPath)

  new("JDBCDriver",
    driverClass = driverClass,
    j_drv = j_drv,
    read_conversions = read_conversions,
    write_conversions = write_conversions,
    info = driver_info(j_drv, dialect, driverClass),
    dialect = dialect
  )
}

driver_info <- function(j_drv, dialect, driverClass) {
  major_version = jtry(.jcall(j_drv, "I", "getMajorVersion", check = FALSE))
  minor_version = jtry(.jcall(j_drv, "I", "getMinorVersion", check = FALSE))

  list(
    driver.version = utils::packageVersion("dbj"),
    client.version = paste(major_version, minor_version, sep = "."),
    max.connections = NA, # TODO: Is there a way to get this information from JDBC?
    driver.class = driverClass,
    sql.dialect = dialect$name  
  )
}

#' @describeIn JDBCDriver JDBC maintains no list of acitve connections. Returns an empty list.
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored.
#' @export
setMethod("dbListConnections", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    list()
  }
)

#' @describeIn JDBCDriver Unloading has no effect but returns always \code{TRUE}.
#' @export
setMethod("dbUnloadDriver", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    invisible(TRUE)
  },
  valueClass = "logical"
)

#' Create a JDBC connection.
#'
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @param url the url to connect to
#' @param user the user to log in
#' @param password the users password
#' @param ... named values which transformed into key-value pairs of of a Java Properties object which is passed to the connect method.
#' @export
setMethod("dbConnect", signature(drv = "JDBCDriver"),
  function(drv, url, user = '', password = '', ...) {
    j_con <- create_jdbc_connection(drv@j_drv, url, user, password, ...)
    JDBCConnection(j_con, drv)
  },
  valueClass = "JDBCConnection"
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
  },
  valueClass = "character"
)

#' @describeIn JDBCDriver always \code{TRUE}.
#' @export
setMethod("dbIsValid", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    TRUE
  },
  valueClass = "logical"
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
