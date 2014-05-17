#' @import rJava
#' @import assertthat
NULL

verifyNotNull <- function(j_object, ...) {
  assert_that(is(j_object, "jobjRef"))
  if (is.jnull(j_object)) {
    stop(deparse(substitute(j_object)), " is null", ...)
  }
}

checkException <- function(...) {
  j_exception <- .jgetEx(clear = TRUE)
  if (!is.null(j_exception)) {
    jstop(j_exception)
  }
}

jstop <- function(j_exception, ...) {
  assert_that(is(j_object, "jobjRef"))
  exception_message <- .jcall(j_exception, "S", "toString")
  stop(..., "Caused by: ", exception_message, call. = FALSE)
}

jtry <- function(expression, onError = jstop, ...) {
  assert_that(is.function(onError))
  .jcheck()
  env <- new.env(parent = parent.frame())
  eval_result <- eval(substitute(expression), env)
  j_exception <- .jgetEx(clear = TRUE)
  if (!is.null(j_exception)) {
    do.call(onError, c(j_exception, list(...)), envir = env)
  }
  return(eval_result)
}