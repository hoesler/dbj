# RJDBC
RJDBC is a [R](http://cran.r-project.org/) package implementing the [DBI](http://cran.r-project.org/web/packages/DBI/) interface using [JDBC](http://www.oracle.com/technetwork/java/javase/jdbc/index.html) via [rJava](http://www.rforge.net/rJava/).

This version is a fork from Simon Urbanek's original RJDBC packge. Apart from minor code modifications I refactored the package layout to meet the requirements of the [devtools](https://github.com/hadley/devtools) package for good R packages. See also the section *Package Development* in the online book [Advanced R programming](http://adv-r.had.co.nz/).

At the moment this a just a playgound for myself but might be merged back later on.

## Installation
This fork can be installed most easily using the [devtools](https://github.com/hadley/devtools) package. After you installed this package just enter `devtools::install_github("hoesler/RJDBC")`.

The source compilation requires that you haven [maven](https://maven.apache.org/) installed.

##	Type conversion
When creating a new table, the follwing type conversion is used:

- logical -> BOOLEAN
- numeric -> DOUBLE
- integer -> INTEGER
- character -> VARCHAR(255)
- Date -> DATE
- POSIXt -> TIMESTAMP




 

