package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

public final class Columns {

    private Columns() {
    }

    public static boolean[] getOptionalStates(final Column<? extends Optional<?>> column) {
        return Booleans.toArray(Lists.transform(column, new Function<Optional<?>, Boolean>() {
            @Override
            public Boolean apply(final Optional<?> optional) {
                return !optional.isPresent();
            }
        }));
    }
}
