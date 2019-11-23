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

public class ResultStreamer {
    private static final Logger LOG = LoggerFactory.getLogger(ResultStreamer.class);
    private final ConnectionProvider connectionProvider;
    private String query;

    @Inject
    public ResultStreamer(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public ResultStreamer query(String query) {
        this.query= query;
        return this;
    }

    public Observable<Integer> emission(int bufferSize) {
        return inOrder(createEmission(bufferSize));

    }

    public Observable<Integer> delayedElementEmission(int bufferSize, int delay, TimeUnit timeUnit) {
        return createEmission(bufferSize).concatMap(values -> Observable.from(values).delay(delay, timeUnit));
    }

    private Observable<Integer> inOrder(Observable<List<Integer>> emission) {
        return emission.concatMap(Observable::from);
    }

    private Observable<List<Integer>> createEmission(int bufferSize) {
        QueryExecutor queryExecutor = new QueryExecutor(connectionProvider, query, bufferSize);
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

    private static class QueryExecutor implements Action2<Long, Observer<Observable<? extends Integer>>> {
        private final ConnectionProvider connectionProvider;
        private final String query;
        private final int bufferSize;
        private ResultSet resultSet;

        public QueryExecutor(ConnectionProvider connectionProvider, String query, int bufferSize) {
            this.connectionProvider = connectionProvider;
            this.query = query;
            this.bufferSize = bufferSize;
        }

        @Override
        public void call(Long requested, Observer<Observable<? extends Integer>> observer) {
            if (resultSet == null) {
                try {
                    resultSet = initialize(bufferSize);
                } catch (Exception e) {
                    observer.onError(e);
                    return;
                }
            }
            try {
                LOG.info("Requested: " + requested);
                Integer[] values = new Integer[requested.intValue()];
                boolean gotNext;
                int counter = -1;
                while ((gotNext = resultSet.next()) && counter < requested) {
                    if(counter == -1) {
                        counter = 0;
                    }
                    values[counter] = resultSet.getInt(1);
                    counter = counter +1;
                }
                LOG.info("Counter reached " + counter);
                if (counter > 0) {
                    LOG.info("Emitting " + counter);
                    observer.onNext(Observable.from(Arrays.copyOf(values, counter)));
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
