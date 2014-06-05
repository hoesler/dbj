package info.urbanek.Rpackage.RJDBC;

enum ColumnType {
    /**
     * column type: string.
     */
    STRING(0),
    /**
     * column type: numeric (retrieved as doubles).
     */
    NUMERIC(1),
    INTEGER(2),
    DATE(3),
    TIMESTAMP(4),
    BOOLEAN(5);


    private final int numericValue;

    ColumnType(final int i) {
        this.numericValue = i;
    }

    /**
     * The numeric value of the column type as returned by {@link java.sql.ResultSetMetaData#getColumnType(int)}.
     *
     * @return the numeric value of the column type
     */
    public int getNumericValue() {
        return numericValue;
    }
}
