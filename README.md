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
# Connect to an H2 database
classpath <- '~/h2-1.3.176.jar' # Use a local jar ...
classpath <- resolve(module('com.h2database:h2:1.3.176'), list(maven_local, maven_central)) # ... or fetch from a maven repository.
drv <- dbj::driver('org.h2.Driver', classpath)
con <- dbConnect(drv, 'jdbc:h2:mem:', user = '', password = '')
```

## Status
dbj is under active development and tested agains different database drivers using [DBItest](https://github.com/rstats-db/DBItest):

- [H2](tests/testthat/test-DBItest-H2.R) (Fully functional)
- [Apache Derby](tests/testthat/test-DBItest-Derby.R) (Functional with minor issues)
- [MySQL Connector/J](tests/testthat/test-DBItest-MySQL.R) (Currently unsupported, because the driver does not support parameter metadata)
- more to come...

##	Type mapping
Type mapping in dbj has four data type units: The R working type, The R transfer type, the Java transfer type and the SQL storage Type. The working type is the type of a data.frame column you work with on the front end. For data transfer, these data types must be converted into an R transfer type, which is associated with one of the Java transfer types. Both transfer types are used to send data from R to Java and vice versa. Due to rJava and performance reasons this must be one of the Java raw types (boolean, byte, int, long, float, double) or String.

The way the data transfer is implemented, it is required that all data that should be transferred must be convertible to one of these transfer types.

The default mapping is defined as follows:

##### Transfer unit
R Transfer Type | Java Transfer Type | SQL Types
----------------|--------------------|----------------------------------------------------------
logical         | boolean            | BIT, BOOLEAN
numeric         | long               | BIGINT, DATE, TIMESTAMP, TIME
numeric         | double             | FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL
integer         | int                | TINYINT, SMALLINT, INTEGER
character       | String             | CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR
list(raw)       | byte[][]           | BINARY, BLOB

##### JDBC -> R
SQL Types                         | R Type     | Transfer unit conversion
----------------------------------|------------|--------------------------------------------------------------------
BIT, BOOLEAN                      | logical    | identity             
FLOAT, REAL, DOUBLE,
NUMERIC, DECIMAL, BIGINT          | numeric    | identity
TINYINT, SMALLINT, INTEGER        | integer    | identity
DATE                              | Date       | as.Date(x / 1000 / 60 / 60 / 24, origin = "1970-01-01", tz = "GMT")
TIME                              | difftime   | as.difftime(data / 1000, units = "secs")
TIMESTAMP                         | POSIXct    | as.POSIXct(x / 1000, origin = "1970-01-01", tz = "GMT")
CHAR, VARCHAR, LONGVARCHAR,
NCHAR, NVARCHAR, LONGNVARCHAR     | character  | identity
BINARY, BLOB                      | list(raw)  | lapply(x, as.raw)

##### R -> JDBC
R Working Type | Colum created as | Transfer unit conversion
---------------|------------------|------------------------------------
logical        | BOOLEAN          | identity
numeric        | DOUBLE PRECISION | identity
integer        | INTEGER          | identity
factor         | VARCHAR(255)     | as.character
character      | VARCHAR(255)     | identity
Date           | DATE             | as.numeric(x) * 24 * 60 * 60 * 1000
difftime       | TIME             | as.numeric(data, units = "secs") * 1000
POSIXct        | TIMESTAMP        | as.numeric(x) * 1000
list(raw)      | BLOB             | lapply(x, as.raw)

## History
dbj is a rewrite of Simon Urbanek's [RJDBC](https://github.com/s-u/dbj) packge. Development started with only minor code modifications to meet the requirements of the [devtools](https://github.com/hadley/devtools) package development tools and the design guidelines for good R packages (See [R packages](http://r-pkgs.had.co.nz/) by Hadley Wickham). In the end, the code diverged too far to merge it back into RJDBC.
