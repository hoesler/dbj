# dbj 0.3

- DBI 0.4 compliant
	- Implemented `dbGetStatement`
	- Implemented `dbGetRowsAffected`
	- Added `dbBind`, which won't be supported for now. It requires too many changes of the current code/object structure. Calls to the current implementation stop with an error. Binding, however, is still supported in `dbSendQuery` and `dbGetQuery` which might also become part of the DBI API ([https://github.com/rstats-db/DBI/issues/25](https://github.com/rstats-db/DBI/issues/25)).