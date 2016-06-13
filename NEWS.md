# dbj 0.2.0.9999

- DBI 0.4 compliant
	- Implemented `dbGetStatement`
	- Implemented `dbGetRowsAffected`
	- Added `dbBind`, which won't be supported for now. It requires too many changes of the current code/object structure. Calls to the current implementation stop with an error. Binding, however, is still supported in `dbSendQuery` and `dbGetQuery` which might also become part of the DBI API ([https://github.com/rstats-db/DBI/issues/25](https://github.com/rstats-db/DBI/issues/25)).
	- Support for row names translation
	- Support for temporary tables
	- Added conversion rule for difftime to TIME
- Added savepoint support to transactions
- SQL dialects must be defined in dbConnect instead of at driver construction
- JDBC driver classes must should be registered explicity with jdbc_register_driver
