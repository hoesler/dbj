h2_driver_class <- 'org.h2.Driver'
h2_classpath <- java_classpath(
	module('com.h2database:h2:1.3.176')
)

DBItest::make_context(
	dbj::driver(h2_driver_class, h2_classpath),
	# DATABASE_TO_UPPER=FALSE is required because of https://github.com/rstats-db/DBItest/issues/64
	list(url = "jdbc:h2:mem:dbi-test;DATABASE_TO_UPPER=FALSE", user = 'sa'),
	tweaks = DBItest::tweaks(constructor_name = "driver")
)

DBItest::test_all(skip = c(
	# Driver independent
	dbj_skips_global,

	# H2 specific
	"data_date($|_.+)",									# Not an error: date() function is undefined in H2.
	"data_timestamp_parens($|_.+)", 		# Not an error: datetime() is an unsupported function in H2
	"data_raw($|_.+)",									# Not an error: SELECT cast(1 as BLOB) is invalid sytax in H2	
	NULL
))

test_bdj_extras(skip = c(),
	default_driver_args = list(driverClass = h2_driver_class, classPath = h2_classpath)
)
