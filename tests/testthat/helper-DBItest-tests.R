test_bdj_extras <- function(skip = NULL, ctx = get_default_context(), default_driver_args = list()) {
  attach(getNamespace("DBItest"), warn.conflicts = FALSE)
  
  test_suite <- "bdj specific"
  
  tests <- list(
	
  	# ajusted version of constructor test in DBItest::test_driver
  	constructor = function() {
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

  	test_truncate = function() {
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

    test_truncate_delete = function() {
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

    test_custom_conversion1 = function() {
    	
      # given
  	  read_conversions <- c(list(
  	    read_conversion_rule(
          function(jdbc.type, ...) with(JDBC_SQL_TYPES,
            jdbc.type == DOUBLE),
          function(...) "numeric",
          function(data, ...) data + 1
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
  	  drv <- do.call(dbj::driver, modifyList(default_driver_args,
        list(read_conversions = read_conversions, write_conversions = write_conversions)))
  	  
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

    test_custom_conversion2 = function() {

      # given
  	  library(lubridate)
  	  read_conversions <- c(list(
  	    read_conversion_rule(
          function(jdbc.type, ...) with(JDBC_SQL_TYPES,
            jdbc.type == DOUBLE),
          function(...) "Duration",
          function(data, ...) duration(data)
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
  	  drv <- do.call(dbj::driver, modifyList(default_driver_args,
        list(read_conversions = read_conversions, write_conversions = write_conversions)))
  	  
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

  run_tests(tests, skip, test_suite, ctx$name)

  detach(getNamespace("DBItest"))
}