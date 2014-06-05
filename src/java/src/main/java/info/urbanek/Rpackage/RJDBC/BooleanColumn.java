package info.urbanek.Rpackage.RJDBC;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class BooleanColumn extends AbstractList<Boolean> implements Column<Boolean> {

    private final static Boolean NA = Boolean.FALSE;
    private final List<Optional<Boolean>> data;

    BooleanColumn() {
        data = new ArrayList<Optional<Boolean>>();
    }

    BooleanColumn(final Collection<Optional<Boolean>> values) {
        data = new ArrayList<Optional<Boolean>>(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Boolean get(final int i) {
        return data.get(i).get();
    }

    public ColumnType getColumnType() {
        return ColumnType.BOOLEAN;
    }

    public boolean[] toBooleanArray() {
        final boolean[] booleans = new boolean[size()];
        for (int i = 0; i < data.size(); i++) {
            booleans[i] = data.get(i).or(NA);
        }
        return booleans;
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final Boolean aBoolean = resultSet.getBoolean(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<Boolean>absent());
        } else {
            data.add(Optional.of(aBoolean));
        }
    }

    @Override
    public void update(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Boolean> aBoolean = data.get(columnIndex);
        if (!aBoolean.isPresent()) {
            statement.setNull(statementIndex, Types.BOOLEAN);
        } else {
            statement.setBoolean(statementIndex, aBoolean.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<Boolean>, Boolean>() {
            @Override
            public Boolean apply(final Optional<Boolean> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    /**
     * Create a new BooleanColumn from an array of boolean values.
     *
     * @param booleans Boolean values
     * @param na       NA indicators
     * @return a new Date column
     */
    public static BooleanColumn create(boolean[] booleans, boolean[] na) {
        checkNotNull(booleans);
        checkNotNull(na);
        checkArgument(booleans.length == na.length);

        final ArrayList<Optional<Boolean>> optionals = new ArrayList<Optional<Boolean>>(booleans.length);
        for (int i = 0; i < booleans.length; i++) {
            optionals.add(na[i] ? Optional.<Boolean>absent() : Optional.of(booleans[i]));
        }
        return new BooleanColumn(optionals);
    }
}
