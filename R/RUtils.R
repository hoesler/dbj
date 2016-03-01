#' Partition data into chunks.
#' 
#' Partition a vector, list matrix or data.frame into a list of vectors, lists, matrices or data.frames respectively,
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