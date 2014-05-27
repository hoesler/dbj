\name{sql_escape}
\alias{sql_escape}
\title{Escape a string so that is is safe to use inside a SQL query.}
\usage{
sql_escape(string, identifier = FALSE, quote = "\\"")
}
\arguments{
  \item{string}{a character vector}

  \item{identifier}{a logical value idicating if
  \code{string} is an identifier}

  \item{quote}{the quoting character}
}
\description{
Escape a string so that is is safe to use inside a SQL
query.
}

