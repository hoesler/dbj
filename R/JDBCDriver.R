#' @include JDBCObject.R
#' @include JavaUtils.R
#' @include JDBCMapping.R
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
    jdrv = "jobjRef",
    read_conversions = "list",
    write_conversions = "list"))

#' Legacy JDBCDriver constructor.
#' 
#' This function is present for \code{RJDBC} compatibility. Use \code{\link{driver}} instead. 
#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by the \code{path.sep} variable in \code{\link{.Platform}} which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param ... Ignored.
#' @rdname JDBCDriver-class-legacy
#' @export
#' @keywords internal
JDBC <- function(driverClass = '', classPath = '', ...) {  
  driver(driverClass, classPath, read_conversions = default_read_conversions, write_conversions = default_write_conversions)
}

#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by \code{path.sep} variable in \code{\link{.Platform}} which should get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param read_conversions a list of JDBCReadConversion objects.
#' @param write_conversions a list of JDBCWriteConversion objects.
#' @rdname JDBCDriver-class
#' @export
driver <- function(driverClass = '', classPath = '', read_conversions = default_read_conversions, write_conversions = default_write_conversions) {  
  assert_that(is.character(driverClass))
  assert_that(is.character(classPath))

  ## expand all paths in the classPath
  expanded_paths <- path.expand(unlist(strsplit(classPath, .Platform$path.sep)))
  .jaddClassPath(expanded_paths)
  
  tryCatch(.jfindClass(as.character(driverClass)[1]),
    error = function(e) sprintf("Driver for class '%s' could not be found.", driverClass))

  jdrv <- .jnew(driverClass)
  verifyNotNull(jdrv)

  new("JDBCDriver", driverClass = driverClass, jdrv = jdrv, read_conversions = read_conversions, write_conversions = write_conversions)
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
    jc <- .jcall("java/sql/DriverManager","Ljava/sql/Connection;","getConnection", as.character(url)[1], as.character(user)[1], as.character(password)[1], check = FALSE)
    if (is.jnull(jc) && !is.jnull(drv@jdrv)) {
      # ok one reason for this to fail is its interaction with rJava's
      # class loader. In that case we try to load the driver directly.
      oex <- .jgetEx(TRUE)
      p <- .jnew("java/util/Properties")
      if (length(user) == 1 && nchar(user)) .jcall(p,"Ljava/lang/Object;","setProperty","user",user)
      if (length(password) == 1 && nchar(password)) .jcall(p,"Ljava/lang/Object;","setProperty","password",password)
      l <- list(...)
      if (length(names(l))) for (n in names(l)) .jcall(p, "Ljava/lang/Object;", "setProperty", n, as.character(l[[n]]))
      jc <- .jcall(drv@jdrv, "Ljava/sql/Connection;", "connect", as.character(url)[1], p)
    }
    verifyNotNull(jc, "Unable to connect JDBC to ", url)
    JDBCConnection(jc, drv)
  },
  valueClass = "JDBCConnection"
)

#' @describeIn JDBCDriver Returns a list with \code{driver.version}, \code{client.version} and \code{max.connections}.
#' @param dbObj An object of class \code{\linkS4class{JDBCDriver}}
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    major_version = jtry(.jcall(dbObj@jdrv, "I", "getMajorVersion", check = FALSE))
    minor_version = jtry(.jcall(dbObj@jdrv, "I", "getMinorVersion", check = FALSE))

    list(
      driver.version = "0.1",
      client.version = paste(major_version, minor_version, sep = "."),
      max.connections = NA # TODO: Is there a way to get this information from JDBC?     
    )
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
    write_conversions <- dbObj@write_conversions
    for (i in seq(length(write_conversions))) {
      if (write_conversions[[i]]$condition(list(class_names = class(obj)))) {
        db_data_type <- write_conversions[[i]]$create_type
        assert_that(is.character(db_data_type) && length(db_data_type == 1))
        return(db_data_type)
      }
    }
    stop("No mapping defined for object of type ", class(obj))
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
