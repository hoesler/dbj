#' Verify that the given \code{j_object} is an object of type \code{jobjRef} but not \code{.jnull()}.
#' 
#' An error is thrown if the verification fails, otherwise \code{NULL} is returned.
#' 
#' @param  j_object an object of type \code{\linkS4class{jobjRef}}
#' @param  ... any further message parts passed to \code{stop}
#' @return \code{NULL} invisibly.
#' @keywords internal
verifyNotNull <- function(j_object, ...) {
  assert_that(is(j_object, "jobjRef"))
  if (is.jnull(j_object)) {
    stop(deparse(substitute(j_object)), " is null: ", ...)
  }
  invisible(NULL)
}

#' Check for any pending java exception.
#' 
#' An error is thrown if one is found, otherwise \code{NULL} is returned.
#' 
#' @return \code{NULL} invisibly.
#' @keywords internal
checkException <- function() {
  j_exception <- .jgetEx(clear = TRUE)
  if (!is.null(j_exception)) {
    jstop(j_exception)
  }
  invisible(NULL)
}

#' Throw an error with the given \code{j_exception} as the cause.
#' 
#' @param j_exception a java Throwable which is converted to a part of the message
#' @param expression a R expression to evaluate
#' @param  ... any further message parts passed to \code{stop}
#' @keywords internal
jstop <- function(j_exception, expression, ...) {
  assert_that(j_exception %instanceof% "java.lang.Throwable")
  exception_message <- expression
  j_throwable <- j_exception
  while (!is.jnull(j_throwable)) {
    exception_message <- c(exception_message, .jcall(j_throwable, "S", "toString"))
    j_throwable <- .jcall(j_throwable, "Ljava/lang/Throwable;", "getCause")
  }
  stop(..., "Caused by: ", paste0(exception_message, collapse = " "), call. = FALSE)
}

#' Wrap an R expression which calls rJava functions.
#' 
#' Execute the given \code{expression} and check for a Java exception afterwards.
#' If an exception was found throw an error.
#' Make sure that the expression, which is usually a .jcall or a .jnew function, is called with the check = FALSE option.
#' Otherwise the exception is cleared before jtry checks for its existance.
#' 
#' @param  expression a valid R expression, usually containing a .jcall
#' @param  onError the callback which will be called if a java exception was thrown with the exception as the first argument
#' @param  ... any further arguments to the \code{onError} function
#' @return the result of the \code{expression}
#' @keywords internal
jtry <- function(expression, onError = jstop, ...) {
  assert_that(is.function(onError))
  .jcheck()
  env <- new.env(parent = parent.frame())
  eval_result <- eval(substitute(expression), env)
  j_exception <- .jgetEx(clear = TRUE)
  if (!is.null(j_exception)) {
    do.call(onError, c(list(j_exception, deparse(substitute(expression))), list(...)), envir = env)
  }
  return(eval_result)
}

