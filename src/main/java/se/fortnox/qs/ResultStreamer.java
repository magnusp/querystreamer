package se.fortnox.qs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action2;
import rx.observables.AsyncOnSubscribe;
import rx.schedulers.Schedulers;
import se.fortnox.reactivewizard.db.ConnectionProvider;

import javax.inject.Inject;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class ResultStreamer<R> {
    private static final Logger         LOG = LoggerFactory.getLogger(ResultStreamer.class);
    private final ConnectionProvider    connectionProvider;
    private String                      query;
    private Consumer<PreparedStatement> parameterBinder;
    private Function<ResultSet, R> resultSetMapper;

    @Inject
    public ResultStreamer(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public ResultStreamer query(String query) {
        this.query= query;
        this.parameterBinder = (statement) -> {};
        this.resultSetMapper = (resultSet) -> null;
        return this;
    }

    public ResultStreamer<R> query(String query, Consumer<PreparedStatement> parameterBinder, Function<ResultSet, R> resultSetMapper) {
        this.query = query;
        this.parameterBinder = parameterBinder;
        this.resultSetMapper = resultSetMapper;
        return this;
    }

    public Observable<R> emission(int bufferSize) {
        return inOrder(createEmission(bufferSize));

    }

    public Observable<R> delayedElementEmission(int bufferSize, int delay, TimeUnit timeUnit) {
        return createEmission(bufferSize).concatMap(values -> Observable.from(values).delay(delay, timeUnit));
    }

    private Observable<R> inOrder(Observable<List<R>> emission) {
        return emission.concatMap(Observable::from);
    }

    private Observable<List<R>> createEmission(int bufferSize) {
        QueryExecutor queryExecutor = new QueryExecutor<R>(connectionProvider, query, parameterBinder, resultSetMapper, bufferSize);
        return Observable
                .create(AsyncOnSubscribe.createStateless(queryExecutor, () -> {
                    try {
                        queryExecutor.abort();
                    } catch (SQLException e) {
                        LOG.error("Exception", e);
                    }
                }))
                .subscribeOn(Schedulers.io())
                .buffer(bufferSize);
    }

    private class QueryExecutor<R> implements Action2<Long, Observer<Observable<R>>> {
        private final ConnectionProvider          connectionProvider;
        private final String                      query;
        private final Function<ResultSet, R> resultMapper;
        private final int bufferSize;
        private ResultSet                         resultSet;
        private Consumer<PreparedStatement> parameterBinder;

        public QueryExecutor(ConnectionProvider connectionProvider,
            String query,
            Consumer<PreparedStatement> parameterBinder,
            Function<ResultSet, R> resultMapper,
            int bufferSize
        ) {
            this.connectionProvider = connectionProvider;
            this.query = query;
            this.parameterBinder = parameterBinder;
            this.resultMapper = resultMapper;

            this.bufferSize = bufferSize;
        }

        @Override
        public void call(Long requested, Observer<Observable<R>> observer) {
            if (resultSet == null) {
                try {
                    resultSet = initialize(bufferSize);
                } catch (Exception e) {
                    observer.onError(e);
                    return;
                }
            }
            try {
                LOG.info("Requested: {}", requested);
                Object[] values = new Object[requested.intValue()];
                boolean gotNext;
                int counter = -1;
                while ((gotNext = resultSet.next()) && counter < requested) {
                    if(counter == -1) {
                        counter = 0;
                    }
                    final R mapped = (R)resultSetMapper.apply(resultSet);
                    values[counter] = mapped;
                    counter = counter +1;
                }
                LOG.info("Counter reached {}", counter);
                if (counter > 0) {
                    LOG.info("Emitting {}", counter);
                    observer.onNext(Observable.from((R[])Arrays.copyOf(values, counter)));
                }
                if (!gotNext) {
                    LOG.info("Reached last row - completing.");
                    observer.onCompleted();
                    dispose(resultSet);
                }

            } catch (SQLException sqlException) {
                try {
                    dispose(resultSet);
                    observer.onError(sqlException);
                } catch (SQLException innerSqlException) {
                    innerSqlException.setNextException(sqlException);
                    observer.onError(innerSqlException);
                }
            }
        }

        private void dispose(ResultSet thing) throws SQLException {
            Statement statement = thing.getStatement();
            Connection connection = statement.getConnection();
            connection.close();
        }

        private ResultSet initialize(int bufferSize) throws SQLException {
            Connection connection = connectionProvider.get();
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            parameterBinder.accept(preparedStatement);
            preparedStatement.setFetchSize(bufferSize);
            preparedStatement.closeOnCompletion();

            return preparedStatement.executeQuery();
        }

        public void abort() throws SQLException {
            LOG.info("Stopping due to upstream unsubscribed");
            dispose(resultSet);
        }
    }
}
