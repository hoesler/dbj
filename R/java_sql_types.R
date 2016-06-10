#' A named list of SQL types as defined in the java class
#' \href{https://docs.oracle.com/javase/7/docs/api/java/sql/Types.html}{java.sql.Types}
#' @export
JDBC_SQL_TYPES <- list(
  BIT = -7L,
  TINYINT = -6L,
  SMALLINT = 5L,
  INTEGER = 4L,
  BIGINT = -5L,
  FLOAT = 6L,
  REAL = 7L,
  DOUBLE = 8L,
  NUMERIC = 2L,
  DECIMAL = 3L,
  CHAR = 1L,
  VARCHAR = 12L,
  LONGVARCHAR = -1L,
  DATE = 91L,
  TIME = 92L,
  TIMESTAMP = 93L,
  BINARY = -2L,
  VARBINARY = -3L,
  LONGVARBINARY = -4L,
  NULL = 0L,
  OTHER = 1111L,
  JAVA_OBJECT = 2000L,
  DISTINCT = 2001L,
  STRUCT = 2002L,
  ARRAY = 2003L,
  BLOB = 2004L,
  CLOB = 2005L,
  REF = 2006L,
  DATALINK = 70L,
  BOOLEAN = 16L,
  # JDBC 4.0
  ROWID = -8L,
  NCHAR = -15L,
  NVARCHAR = -9L,
  LONGNVARCHAR = -16L,
  NCLOB = 2011L,
  SQLXML = 2009L
)

as.jdbc_sql_type <- function(x, ...) UseMethod("as.jdbc_sql_type")
as.jdbc_sql_type.numeric <- function(x, ...) {
  assert_that(all(x %in% JDBC_SQL_TYPES))
  x
}
as.jdbc_sql_type.character <- function(x, ...) {
  longest_prefix <- max(names(JDBC_SQL_TYPES)[which(startsWith(x, names(JDBC_SQL_TYPES)))])
  JDBC_SQL_TYPES[[longest_prefix]]
}
