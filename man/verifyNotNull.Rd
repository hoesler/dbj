\name{verifyNotNull}
\alias{verifyNotNull}
\title{Verify that the given \code{j_object} is an object of type \code{jobjRef} but not \code{.jnull()}.
An error is thrown if the verification fails, otherwise \code{NULL} is returned.}
\usage{
verifyNotNull(j_object, ...)
}
\arguments{
  \item{j_object}{an object of type
  \code{\linkS4class{jobjRef}}}
}
\value{
NULL if the verification succeded
}
\description{
Verify that the given \code{j_object} is an object of type
\code{jobjRef} but not \code{.jnull()}. An error is thrown
if the verification fails, otherwise \code{NULL} is
returned.
}

