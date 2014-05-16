#' @include JDBCObject.R
NULL

.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc=libname)
}

#' Class JDBCDriver with factory method JDBC.
#'
#' @name JDBCDriver-class
#' @docType class
#' @exportClass JDBCDriver
setClass("JDBCDriver",
  contains = c("DBIDriver", "JDBCObject"),
  slots = c(
    identifier.quote = "character",
    jdrv = "jobjRef"))

#' @param driverClass the java class name of the JDBC driver to use
#' @param classPath a string of paths seperated by : which shoul get added to the classpath (see \link[rJava]{.jaddClassPath})
#' @param identifier.quote the quoting character to quote identifiers
#' @rdname JDBCDriver-class
#' @export
JDBC <- function(driverClass = '', classPath = '', identifier.quote = NA) {  
  ## expand all paths in the classPath
  expanded_paths <- path.expand(unlist(strsplit(classPath, .Platform$path.sep)))
  .jaddClassPath(expanded_paths)
  
  if (nchar(driverClass) && is.jnull(.jfindClass(as.character(driverClass)[1]))) {
    stop("Cannot find JDBC driver class ",driverClass)
  }

  jdrv <- .jnew(driverClass)

  new("JDBCDriver", identifier.quote = as.character(identifier.quote), jdrv = jdrv)
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

#' Unload JDBCDriver driver.
#' 
#' @param drv Object created by \code{\link{JDBC}}
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A logical indicating whether the operation succeeded or not.
#' @export
setMethod("dbUnloadDriver", signature(drv = "JDBCDriver"),
  function(drv, ...) {
    warning("Unloading a JDBCDriver has no effect")
    TRUE
  },
  valueClass = "logical"
)

#' Create a JDBC connection.
#'
#' @param drv Object created by \code{\link{JDBC}}
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
    verifyNotNull(jc, "Unable to connect JDBC to ",url)
    new("JDBCConnection", jc = jc, identifier.quote = drv@identifier.quote)
  },
  valueClass = "JDBCConnection"
)


#' @export
setMethod("summary", signature(object = "JDBCDriver"),
  function(object, ...) {
    jdbcDescribeDriver(object, ...)
  }
)

jdbcDescribeDriver <- function(obj, verbose = FALSE, ...) {
  info <- dbGetInfo(obj)
  show(obj)
  cat("  Driver name: ", info$name, "\n")
  cat("  Driver version: ", info$driver.version, "\n")
  invisible(NULL)
}

#' @export
setMethod("dbGetInfo", signature(dbObj = "JDBCDriver"),
  function(dbObj, ...) {
    callNextMethod()
  }
)
