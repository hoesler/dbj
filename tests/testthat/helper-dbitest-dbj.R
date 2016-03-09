test_bdj_extras <- function(skip = NULL, ctx = get_default_context(), default_driver_args = list()) {
  attach(getNamespace("DBItest"), warn.conflicts = FALSE)
  
  test_suite <- "bdj specific"
  
  tests <- list(
  	test_keyords = function() {
  	  with_connection({
  	  	# when
		keywords <- SQLKeywords(con)
		
		# then
		expect_is(keywords, "character")
		# H2 keywords (see http://h2database.com/javadoc/org/h2/jdbc/JdbcDatabaseMetaData.html#getSQLKeywords)
		expect_true(all(c("LIMIT","MINUS","ROWNUM","SYSDATE","SYSTIME","SYSTIMESTAMP","TODAY") %in% keywords))
		expect_true(all(.SQL92Keywords %in% keywords))
  	  })
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
		    sqltype_read_conversion(JDBC_SQL_TYPES$DOUBLE, "numeric", function(x) x + 1)
		  ), default_read_conversions)

		  write_conversions <- c(list(
		    mapped_write_conversion("numeric", function(x) x + 1, "DOUBLE")
		  ), default_write_conversions)

		  # when
		  drv <- do.call(dbj::driver, modifyList(default_driver_args, list(read_conversions = read_conversions, write_conversions = write_conversions)))
		  
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
		    sqltype_read_conversion(JDBC_SQL_TYPES$DOUBLE, "Duration", function(x) duration(x))
		  ), default_read_conversions)

		  write_conversions <- c(list(
		    mapped_write_conversion("Duration", function(x) as.numeric(x), "DOUBLE")
		  ), default_write_conversions)

		  # when
		  drv <- do.call(dbj::driver, modifyList(default_driver_args, list(read_conversions = read_conversions, write_conversions = write_conversions)))
		  
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

  run_tests(tests, skip, test_suite)

  detach(getNamespace("DBItest"))
}