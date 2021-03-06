#' Partition data into chunks
#' 
#' Partition a vector, list matrix or data.frame into a list of vectors,
#' lists, matrices or data.frames respectively,
#' each of the same size (last might be smaller).
#' 
#' @param data the data to partition
#' @param size the partition size
#' @export
partition <- function(data, size) UseMethod("partition")

#' @export
#' @describeIn partition Partition a data.frame
partition.data.frame <- function(data, size) {
  assert_that(is.data.frame(data))
  assert_that(is.number(size), length(size) == 1, as.integer(size) == size, size > 0)
  
  if (size >= nrow(data)) {
    return(list(data))
  } else {
    lapply(seq(ceiling(nrow(data) / size)), function(i) {
      data[seq((i - 1) * size + 1, min(i * size, nrow(data))), , drop = FALSE]
    })
  }
}

#' @export
#' @describeIn partition Default partition function
partition.default <- function(data, size) {
  assert_that(is.vector(data) || is.list(data))
  assert_that(is.number(size), length(size) == 1, as.integer(size) == size, size > 0)

  if (size >= nrow(data)) {
    return(list(data))
  } else {
    lapply(seq(ceiling(length(data) / size)), function(i) {
      data[seq((i - 1) * size + 1, min(i * size, length(data)))]
    })
  }
}

requireOption <- function(x) {
  if (!x %in% names(options())) stop(sprintf("Option %s is not present", x))
  getOption(x)
}

#' Covert a character vector into a classpath
#' 
#' Concatenate all elements in \code{paths} using \code{.Platform$path.sep} as the separator.
#' 
#' @param paths A character vector
#' @return A character vector of the concatenated paths.
#' @export
as_classpath <- function(paths) {
  paste0(paths, collapse = .Platform$path.sep)
}

is.raw_list <- function(x) {
  all(vapply(x, function(x) { is.raw(x) || is.na(x) }, logical(1)))
}

is.difftime <- function(x) is(x, "difftime")

is.Date <- function(x) is(x, "Date")

is.POSIXct <- function(x) is(x, "POSIXct")
