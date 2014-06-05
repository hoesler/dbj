package info.urbanek.Rpackage.RJDBC;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import java.sql.*;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class StringColumn extends AbstractList<String> implements Column<String> {

    private static final String NA = "NA";
    private final List<Optional<String>> data;

    StringColumn() {
        data = new ArrayList<Optional<String>>();
    }

    StringColumn(final Collection<Optional<String>> values) {
        data = new ArrayList<Optional<String>>(values);
    }

    public ColumnType getColumnType() {
        return ColumnType.STRING;
    }

    public String[] toStringArray() {
        return toArray(new String[size()]);
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final String string = resultSet.getString(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<String>absent());
        } else {
            data.add(Optional.of(string));
        }
    }

    @Override
    public void update(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<String> cast = data.get(columnIndex);
        if (!cast.isPresent()) {
            statement.setNull(statementIndex, Types.VARCHAR);
        } else {
            statement.setString(statementIndex, cast.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<String>, Boolean>() {
            @Override
            public Boolean apply(final Optional<String> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    @Override
    public String get(final int i) {
        return data.get(i).orNull();
    }

    @Override
    public int size() {
        return data.size();
    }

    /**
     * Create a new StringColumn from an array of Strings.
     *
     * @param values String values
     * @param na     NA indicators
     * @return a new Date column
     */
    public static StringColumn create(String[] values, boolean[] na) {
        checkNotNull(values);
        checkNotNull(na);
        checkArgument(values.length == na.length);

        final ArrayList<Optional<String>> strinOptionals = new ArrayList<Optional<String>>(values.length);
        for (int i = 0; i < values.length; i++) {
            strinOptionals.add(na[i] ? Optional.<String>absent() : Optional.of(values[i]));
        }
        return new StringColumn(strinOptionals);
    }
}
