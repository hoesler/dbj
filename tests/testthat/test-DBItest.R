DBItest::make_context(JDBC('org.h2.Driver', getOption("h2_jar")), list(url = "jdbc:h2:mem:;DATABASE_TO_UPPER=FALSE", user = 'sa'))
# DBItest should use dbQuoteIdentifier instead of using DATABASE_TO_UPPER=FALSE
DBItest::test_result(skip = c(
	"constructor_strict", "constructor", # too restrictive
	"get_query_empty_single_column", "get_query_empty_multi_column", # default implementation of dbGetQuery in 0.3.1 returns NULL for empty results
	"data_logical_int($|_.+)", # I don't understand these tests. Why and when should logical be ints?
	"data_date($|_.+)", # date() is undefined in H2.
	"data_time($|_.+)", # why should time be returned as character?
	"data_timestamp_parens($|_.+)", # datetime is an unsupported function in H2
	"data_timestamp_utc($|_.+)", # If we store timestamp as SQL TIMESTAMP we have to discard timezone information.
	"data_raw($|_.+)" # to be implemented
))