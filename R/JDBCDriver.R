#' @include JDBCObject.R
#' @include JavaUtils.R
#' @include JDBCMapping.R
NULL

#' Class JDBCDriver with factory methods.
#'
#' @export
setClass("JDBCDriver",
  contains = c("DBIDriver", "JDBCObject"),
  slots = c(
    driverClass = "character",
    jdrv = "jobjRef",
    read_conversions = "list",
    write_conversions = "list"))

#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by : which shoul get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param read_conversions a list of RJDBCReadConversion objects.
#' @param write_conversions a list of RJDBCWriteConversion objects.
#' @rdname JDBCDriver-class
#' @export
JDBC <- function(driverClass = '', classPath = '', read_conversions = default_read_conversions, write_conversions = default_write_conversions) {  
  JDBCDriver(driverClass, classPath, read_conversions, write_conversions)
}

#' @rdname JDBCDriver-class
#' @export
JDBCDriver <- function(driverClass = '', classPath = '', read_conversions = default_read_conversions, write_conversions = default_write_conversions) {
  ## expand all paths in the classPath
  expanded_paths <- path.expand(unlist(strsplit(classPath, .Platform$path.sep)))
  .jaddClassPath(expanded_paths)
  
  if (nchar(driverClass) && is.jnull(.jfindClass(as.character(driverClass)[1]))) {
    stop("Cannot find JDBC driver class ",driverClass)
  }

  jdrv <- .jnew(driverClass)

  new("JDBCDriver", driverClass = driverClass, jdrv = jdrv, read_conversions = read_conversions, write_conversions = write_conversions)
}

#' List active connections
#' 
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("dbListConnections", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    warning("JDBC driver maintains no list of acitve connections.")
    list()
  }
)

#' Unload \code{\linkS4class{JDBCDriver}} driver.
#' 
#' @param drv An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A logical indicating whether the operation succeeded or not.
#' @export
setMethod("dbUnloadDriver", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    warning("Unloading a JDBCDriver has no effect")
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

#' Print a summary for the driver
#'
#' @param object An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
setMethod("summary", signature(object = "JDBCDriver"),
  function(object, ...) {
    info <- dbGetInfo(object)
    cat("JDBC Driver\n")
    cat(sprintf("  Driver class: %s\n", object@driverClass))
    cat(sprintf("  Driver version: %s.%s\n", info$major_version, info$minor_version))
    invisible(NULL)
  }
)

#' Get meta information about the driver
#'
#' @param dbObj An object of class \code{\linkS4class{JDBCDriver}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A list with information about the driver
#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    list(
      minor_version = jtry(.jcall(dbObj@jdrv, "I", "getMajorVersion", check = FALSE)),
      major_version = jtry(.jcall(dbObj@jdrv, "I", "getMinorVersion", check = FALSE))
    )
  }
)

setMethod("dbGetDriver", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    dbObj
  }
)

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
