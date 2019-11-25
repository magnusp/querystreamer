package se.fortnox.qs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.observables.AsyncOnSubscribe;
import rx.schedulers.Schedulers;
import se.fortnox.reactivewizard.db.ConnectionProvider;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;

public class ResultStreamer<R> {
    private static final Logger         LOG = LoggerFactory.getLogger(ResultStreamer.class);
    private final ConnectionProvider connectionProvider;
    private final String query;
    private final Consumer<PreparedStatement> parameterBinder;
    private final Function<ResultSet, R> resultSetMapper;


    ResultStreamer(ConnectionProvider connectionProvider, String query, Consumer<PreparedStatement> parameterBinder, Function<ResultSet, R> resultSetMapper) {
        this.connectionProvider = connectionProvider;
        this.query = query;
        this.parameterBinder = parameterBinder;
        this.resultSetMapper = resultSetMapper;
    }

    public Observable<R> emit(int fetchSize) {
        return createEmission(fetchSize);
    }

    private Observable<R> createEmission(int fetchSize) {
        QueryExecutor<R> queryExecutor = new QueryExecutor<>(connectionProvider, query, parameterBinder, resultSetMapper, fetchSize);
        Action0 onUnsubscribe = () -> {
            try {
                queryExecutor.abort();
            } catch (SQLException e) {
                LOG.error("Exception", e);
            }
        };
        return Observable
                .create(AsyncOnSubscribe.createStateless(queryExecutor, onUnsubscribe))
                .subscribeOn(Schedulers.io());
    }

}
