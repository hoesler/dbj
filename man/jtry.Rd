\name{jtry}
\alias{jtry}
\title{Execute the given \code{expression} and check for a Java exception afterwards.
If an exception was found throw an error.
Make sure that the expression, which is usually a .jcall or a .jnew function, is called with the check = FALSE option.
Otherwise the exception is cleared before jtry checks for its existance.}
\usage{
jtry(expression, onError = jstop, ...)
}
\arguments{
  \item{expression}{a valid R expression, usually
  containing a .jcall}

  \item{onError}{the callback which will be called if a
  java exception was thrown with the exception as the first
  argument}

  \item{...}{any further arguments to the \code{onError}
  function}
}
\value{
the result of the \code{expression}
}
\description{
Execute the given \code{expression} and check for a Java
exception afterwards. If an exception was found throw an
error. Make sure that the expression, which is usually a
.jcall or a .jnew function, is called with the check =
FALSE option. Otherwise the exception is cleared before
jtry checks for its existance.
}

