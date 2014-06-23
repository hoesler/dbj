#' Partition a vector, list matrix or data.frame into a list of vectors, lists, matrices or data.frames
#' each of the same size (last might be smaller).
#' @param data the data to partition
#' @param size the partition size
#' @export
partition <- function(data, size) UseMethod("partition")

#' @export
#' @rdname partition
partition.data.frame <- function(data, size) {
  lapply(seq(ceiling(nrow(data) / size)), function(i) {
    data[seq((i - 1) * size + 1, min(i * size, nrow(data))), , drop = FALSE]
  })
}