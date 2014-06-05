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

public final class IntegerColumn extends AbstractList<Integer> implements Column<Integer> {

    private final static Integer NA = Integer.MIN_VALUE;
    private final List<Optional<Integer>> data;

    IntegerColumn() {
        data = new ArrayList<Optional<Integer>>();
    }

    IntegerColumn(final Collection<Optional<Integer>> values) {
        data = new ArrayList<Optional<Integer>>(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Integer get(final int i) {
        return data.get(i).orNull();
    }

    public ColumnType getColumnType() {
        return ColumnType.INTEGER;
    }

    public int[] toIntegerArray() {
        final int[] ints = new int[size()];
        for (int i = 0; i < data.size(); i++) {
            ints[i] = data.get(i).or(NA);
        }
        return ints;
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final int anInt = resultSet.getInt(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<Integer>absent());
        } else {
            data.add(Optional.of(anInt));
        }
    }

    @Override
    public void update(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Integer> cast = data.get(columnIndex);
        if (!cast.isPresent()) {
            statement.setNull(statementIndex, Types.INTEGER);
        } else {
            statement.setInt(statementIndex, cast.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<Integer>, Boolean>() {
            @Override
            public Boolean apply(final Optional<Integer> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    /**
     * Create a new IntegerColumn from an array of integer values.
     *
     * @param integers integer values
     * @param na       NA indicators
     * @return a new Date column
     */
    public static IntegerColumn create(int[] integers, boolean[] na) {
        checkNotNull(integers);
        checkNotNull(na);
        checkArgument(integers.length == na.length);

        final ArrayList<Optional<Integer>> optionals = new ArrayList<Optional<Integer>>(integers.length);
        for (int i = 0; i < integers.length; i++) {
            optionals.add(na[i] ? Optional.<Integer>absent() : Optional.of(integers[i]));
        }
        return new IntegerColumn(optionals);
    }
}
