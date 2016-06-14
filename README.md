# dbj

[![Build Status](https://travis-ci.org/hoesler/dbj.svg?branch=master)](https://travis-ci.org/hoesler/dbj)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/hoesler/dbj?branch=master&svg=true)](https://ci.appveyor.com/project/hoesler/dbj)

dbj is an [R](http://cran.r-project.org/) package implementing [DBI](https://github.com/rstats-db/DBI) 0.4 using [JDBC](http://www.oracle.com/technetwork/java/javase/jdbc/index.html) via [rJava](http://www.rforge.net/rJava/).

## Requirements
- Java 6+

## Installation
dbj can be installed most easily using the [devtools](https://github.com/hadley/devtools) package. After you have installed devtools, just run `devtools::install_github("hoesler/dbj")`.

The source compilation requires that you have [maven](https://maven.apache.org/) installed.

## Usage
```R
library(DBI)
library(dbj)

# Initially, register the JDBC drivers you need
jdbc_register_driver(
 'org.h2.Driver',
 resolve(
   module('com.h2database:h2:1.3.176'),
   repositories = list(maven_local, maven_central) )
)

con <- dbConnect(dbj::driver(), 'jdbc:h2:mem:')
dbWriteTable(con, "iris", iris)

# dbj supports prepared statements
sql <- paste0(
  "SELECT * FROM ", dbQuoteIdentifier(con, "iris"),
  " WHERE ", dbQuoteIdentifier(con, "Species"), " = ?"
)
dbGetQuery(con, sql, parameters = list("setosa"))
```

## Status
No version has been releaded yet.
dbj is under active development and tested agains different JDBC drivers using [DBItest](https://github.com/rstats-db/DBItest):

- [H2](tests/testthat/test-DBItest-H2.R)
- [Apache Derby](tests/testthat/test-DBItest-Derby.R)
- [MySQL Connector/J](tests/testthat/test-DBItest-MySQL.R)
- more to come...

##	Type mapping
The default type mapping between R and SQL is (roughly) as follows:

R classes         | SQL types
------------------|-----------------
logical           | BIT, BOOLEAN
numeric           | FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL, BIGINT
integer           | TINYINT, SMALLINT, INTEGER, NULL
character, factor | CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR
Date              | DATE
difftime          | TIME
POSIXct           | TIMESTAMP
list(raw)         | BINARY, BLOB

## Acknowledgements
dbj is a rewrite of Simon Urbanek's [RJDBC](https://github.com/s-u/dbj) packge. Development started with only minor code modifications to meet the requirements of the [devtools](https://github.com/hadley/devtools) package development tools and the design guidelines for good R packages (See [R packages](http://r-pkgs.had.co.nz/) by Hadley Wickham). In the end, the code diverged too far to merge it back into RJDBC.
