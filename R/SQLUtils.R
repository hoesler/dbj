NULL

#' Escape a string so that is is safe to use inside a SQL query.
#' 
#' @param  string a character vector
#' @param  quote the quoting character
#' @param  identifier a logical value idicating if \code{string} is an identifier
#' @export
sql_escape <- function(string, quote, identifier) {
  assert_that(is.character(string))
  assert_that(is.logical(identifier))
  assert_that(is.character(quote))

  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$", string)
    if (length(string[-vid])) {
      string[-vid] <- sql_escape(string[-vid], quote, FALSE)
    }
    string[vid] <- paste(quote, string[vid], quote, sep = '')
    return(string)
  }

  if (is.na(quote)) {
    quote <- ''
  }
  
  string <- gsub("\\\\", "\\\\\\\\", string)
  
  if (nchar(quote)) {
    string <- gsub(paste("\\", quote, sep = ''), paste("\\\\\\", quote, sep = ''), string, perl = TRUE)
  }

  paste(quote, string, quote, sep = '')
}

#' @rdname sql_escape
sql_escape_identifier <- function(string, quote) {
  sql_escape(string, quote = quote, identifier = TRUE)
}

#' @rdname sql_escape
sql_escape_value <- function(string, quote) {
  sql_escape(string, quote = quote, identifier = FALSE)
}