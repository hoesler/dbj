package info.urbanek.Rpackage.RJDBC;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DateColumn extends AbstractList<Date> implements Column<Date> {

    private final static Date NA = new Date(Integer.MIN_VALUE * 24L * 60 * 60 * 1000);
    private final List<Optional<Date>> data;

    DateColumn() {
        data = Lists.newArrayList();
    }

    DateColumn(final Collection<Optional<Date>> values) {
        data = Lists.newArrayList(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Date get(final int i) {
        return data.get(i).orNull();
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.DATE;
    }

    /**
     * @return the column values as an array of days since 01.01.1970
     */
    public int[] toDays() {
        final int size = size();
        final int[] days = new int[size];
        for (int i = 0; i < size; i++) {
            final Date date = data.get(i).or(NA);
            days[i] = (int) (date.getTime() / 24 / 60 / 60 / 1000);
        }
        return days;
    }

    @Override
    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final Date date = resultSet.getDate(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<Date>absent());
        } else {
            data.add(Optional.of(date));
        }
    }

    @Override
    public void update(final PreparedStatement statement, int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Date> dateOptional = data.get(columnIndex);
        if (!dateOptional.isPresent()) {
            statement.setNull(statementIndex, Types.DATE);
        } else {
            statement.setDate(statementIndex, dateOptional.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<Date>, Boolean>() {
            @Override
            public Boolean apply(final Optional<Date> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    /**
     * Create a new DateColumn from an array of date values given as days since 01.01.1970.
     *
     * @param days date values as the difference in days from 01.01.1970
     * @param na   NA indicators
     * @return a new Date column
     */
    public static DateColumn forDays(int[] days, boolean[] na) {
        checkNotNull(days);
        checkNotNull(na);
        checkArgument(days.length == na.length);

        final List<Optional<Date>> optionals = Lists.newArrayList();
        for (int i = 0; i < days.length; i++) {
            optionals.add(na[i] ? Optional.<Date>absent() : Optional.of(new Date(days[i] * 24L * 60 * 60 * 1000)));
        }

        return new DateColumn(optionals);
    }
}
