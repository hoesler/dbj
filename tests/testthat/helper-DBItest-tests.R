test_bdj_extras <- function(skip = NULL, ctx = get_default_context()) {
  attach(getNamespace("DBItest"), warn.conflicts = FALSE)
  
  test_suite <- "bdj specific"
  
  tests <- list(
	
  	# ajusted version of constructor test in DBItest::test_driver
  	constructor = function(ctx) {
      pkg_name <- package_name(ctx)

      constructor_name <- ctx$tweaks$constructor_name %||%
        gsub("^R", "", pkg_name)

      pkg_env <- getNamespace(pkg_name)
      eval(bquote(
        expect_true(.(constructor_name) %in% getNamespaceExports(pkg_env))))
      eval(bquote(
        expect_true(exists(.(constructor_name), mode = "function", pkg_env))))
      constructor <- get(constructor_name, mode = "function", pkg_env)
      #expect_that(constructor, all_args_have_default_values())
    },

  	test_truncate = function(ctx) {
      with_connection({
        on.exit(expect_error(dbRemoveTable(con, "iris"), NA),
                add = TRUE)

        iris <- get_iris(ctx)
        dbWriteTable(con, "iris", iris)

        # when
  	    success <- dbTruncateTable(con, "iris", use_delete = FALSE)

      	# then
      	expect_true(success)
      })
    },

    test_truncate_delete = function(ctx) {
      with_connection({
        on.exit(expect_error(dbRemoveTable(con, "iris"), NA),
                add = TRUE)

        iris <- get_iris(ctx)
        dbWriteTable(con, "iris", iris)

        # when
  	    success <- dbTruncateTable(con, "iris", use_delete = TRUE)

      	# then
      	expect_true(success)
      })
    },

    test_custom_conversion1 = function(ctx) {
    	
      # given
  	  read_conversions <- c(list(
  	    read_conversion_rule(
          function(jdbc.type, ...) with(JDBC_SQL_TYPES,
            jdbc.type == DOUBLE),
          function(data, ...) data + 1,
          function(...) "numeric"
        )
  	  ), default_read_conversions)

  	  write_conversions <- c(list(
  	    write_conversion_rule(
          function(data, ...) is.numeric(data),
          function(data, ...) data + 1,
          function(...) "DOUBLE"
        )
  	  ), default_write_conversions)

  	  # when
  	  drv <- do.call(dbj::driver,
        list(read_conversions = read_conversions, write_conversions = write_conversions))
  	  
  	  # then
  	  con <- do.call(dbConnect, c(list(drv), ctx$connect_args))
  	  on.exit(dbDisconnect(con))
  	  iris <- get_iris(ctx)

  	  iris_read <- tryCatch({
  	  	expect_error(dbGetQuery(con, paste0("SELECT * FROM ", dbQuoteIdentifier("iris"))))
  		  dbWriteTable(con, "iris", iris)
  	  	dbReadTable(con, "iris")
  	  }, finally = dbRemoveTable(con, "iris"))

  	  expect_identical(iris_read$Sepal.Length, iris$Sepal.Length + 2)
  	},

    test_custom_conversion2 = function(ctx) {

      # given
  	  library(lubridate)
  	  read_conversions <- c(list(
  	    read_conversion_rule(
          function(jdbc.type, ...) with(JDBC_SQL_TYPES,
            jdbc.type == DOUBLE),
          function(data, ...) duration(data),
          function(...) "Duration"
        )
  	  ), default_read_conversions)

  	  write_conversions <- c(list(
  	    write_conversion_rule(
          function(data, ...) is.duration(data),
          function(data, ...) as.numeric(data),
          function(...) "DOUBLE"
        )
  	  ), default_write_conversions)

  	  # when
  	  drv <- do.call(dbj::driver,
        list(read_conversions = read_conversions, write_conversions = write_conversions))
  	  
  	  # then
  	  con <- do.call(dbConnect, c(list(drv), ctx$connect_args))
  	  on.exit(dbDisconnect(con))
  	  df <- data.frame(duration = duration(c(0, 10000000, 1/1000)))

  	  df_read <- tryCatch({
  	  	expect_error(dbGetQuery(con, paste0("SELECT * FROM ", dbQuoteIdentifier("time_table"))))
  		  dbWriteTable(con, "time_table", df)
  	  	dbReadTable(con, "time_table")
  	  }, finally = dbRemoveTable(con, "time_table"))

  	  expect_identical(df_read, df)
  	}
  )

  run_tests(ctx, tests, skip, test_suite)

  detach(getNamespace("DBItest"))
}