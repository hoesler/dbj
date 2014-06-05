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

public final class TimestampColumn extends AbstractList<Timestamp> implements Column<Timestamp> {

    private static final Timestamp NA = new Timestamp(Integer.MIN_VALUE * 1000L);
    private final List<Optional<Timestamp>> data;

    TimestampColumn() {
        data = new ArrayList<Optional<Timestamp>>();
    }

    TimestampColumn(final Collection<Optional<Timestamp>> values) {
        data = new ArrayList<Optional<Timestamp>>(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Timestamp get(final int i) {
        return data.get(i).orNull();
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.TIMESTAMP;
    }

    public int[] toSeconds() {
        final int[] ints = new int[size()];
        for (int i = 0; i < size(); i++) {
            final Timestamp timestamp = data.get(i).or(NA);
            assert timestamp != null;
            ints[i] = (int) (timestamp.getTime() / 1000);
        }
        return ints;
    }

    @Override
    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final Timestamp timestamp = resultSet.getTimestamp(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<Timestamp>absent());
        } else {
            data.add(Optional.of(timestamp));
        }
    }

    @Override
    public void update(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Timestamp> timestamp = data.get(columnIndex);
        if (!timestamp.isPresent()) {
            statement.setNull(statementIndex, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(statementIndex, timestamp.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<Timestamp>, Boolean>() {
            @Override
            public Boolean apply(final Optional<Timestamp> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    /**
     * Create a new TimestampColumn column from an array of seconds.
     *
     * @param seconds timestamps as seconds since 01.01.1970 00:00
     * @param na      NA indicators
     * @return a new Date column
     */
    public static TimestampColumn forSeconds(int[] seconds, boolean[] na) {
        checkNotNull(seconds);
        checkNotNull(na);
        checkArgument(seconds.length == na.length);

        final ArrayList<Optional<Timestamp>> doubles = new ArrayList<Optional<Timestamp>>(seconds.length);
        for (int i = 0; i < seconds.length; i++) {
            doubles.add(na[i] ? Optional.<Timestamp>absent() : Optional.of(new Timestamp(seconds[i] * 1000L)));
        }
        return new TimestampColumn(doubles);
    }
}
