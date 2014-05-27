NULL

#' Escape a string so that is is safe to use inside a SQL query.
#' 
#' @param  string a character vector
#' @param  identifier a logical value idicating if \code{string} is an identifier
#' @param  quote the quoting character
#' @export
sql_escape <- function(string, identifier = FALSE, quote = "\"") {
  expect_that(string, is_a("character"))
  expect_that(identifier, is_a("logical"))
  expect_that(quote, is_a("character"))

  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$", string)
    if (length(string[-vid])) {
      if (is.na(quote)) {
        stop("The JDBC connection doesn't support quoted identifiers, 
          but table/column name contains characters that must be quoted (", paste(string[-vid], collapse = ','), ")")
      }
      string[-vid] <- sql_escape(string[-vid], FALSE, quote)
    }
    string[vid] <- paste(quote, string[vid], quote, sep = '')
    return(string)
  }

  if (is.na(quote)) {
    quote <- ''
  }
  
  string <- gsub("\\\\","\\\\\\\\",string)
  
  if (nchar(quote)) {
    string <- gsub(paste("\\",quote,sep = ''),paste("\\\\\\",quote,sep = ''),string,perl = TRUE)
  }

  paste(quote,string,quote,sep = '')
}