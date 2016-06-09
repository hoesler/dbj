#' A DBI implementation using JDBC over rJava.
#'
#' Start with \code{\link{driver}} to create a new database driver.
#' 
#' @docType package
#' @import DBI rJava methods assertthat
#' @name dbj
NULL

.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc = libname)

  # Workaround for devtools::test()
  # .package will not call the overwriten system.file of the devtools environment
  # which takes care of the different folder structure.
  if (!any(grepl("dbj", .jclassPath(), TRUE))) {
    java_folder <- system.file("java", package = pkgname, lib.loc = libname)
    jars <- grep(".*\\.jar", list.files(java_folder, full.names = TRUE), TRUE, value = TRUE)
    .jaddClassPath(jars)
  }  
}