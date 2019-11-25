package se.fortnox.qs;

import se.fortnox.reactivewizard.db.ConnectionProvider;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Consumer;
import java.util.function.Function;

public class ResultStreamerBuilder<R> {
    private final ConnectionProvider connectionProvider;
    private String query;
    private Function<ResultSet, R> resultSetMapper;
    private Consumer<PreparedStatement> parameterBinder = (preparedStatement -> {});

    @Inject
    public ResultStreamerBuilder(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public ResultStreamerBuilder<R> query(String query) {
        this.query = query;
        return this;
    }

    public ResultStreamerBuilder<R> mappedBy(Function<ResultSet, R> resultSetMapper) {
        this.resultSetMapper = resultSetMapper;
        return this;
    }

    public ResultStreamerBuilder<R> boundBy(Consumer<PreparedStatement> parameterBinder) {
        this.parameterBinder = parameterBinder;
        return this;
    }

    public ResultStreamer<R> build() {
        return new ResultStreamer<>(connectionProvider, query, parameterBinder, resultSetMapper);
    }
}
