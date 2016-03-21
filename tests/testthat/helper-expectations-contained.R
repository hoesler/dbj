find_expr <- function(name, env = parent.frame()) {
  subs <- do.call("substitute", list(as.name(name), env))
  paste0(deparse(subs, width.cutoff = 500), collapse = "\n")
}

expect_contained <- function(object, expected, ..., info = NULL, label = NULL, expected.label = NULL) {
	if (is.null(label)) {
      label <- find_expr("object")
  }
  if (is.null(expected.label)) {
      expected.label <- find_expr("expected")
  }
	expect_that(object, is_contained_in(expected, label = expected.label, ...), info = info, label = label)
}

is_contained_in <- function(expected, label = NULL, ...) {
	if (is.null(label)) {
		label <- find_expr("expected")
	} else if (!is.character(label) || length(label) != 1) {
		label <- deparse(label)
	}

	function(actual_elements) {
		contained <- match(actual_elements, expected, ..., nomatch = 0) > 0
		expectation(
			all(contained),
			paste0("has elements not present in ", label, ": [", paste0(actual_elements[!contained], collapse = ", "), "]"),
			paste0("is fully contained in ", label)
		)
	}
}