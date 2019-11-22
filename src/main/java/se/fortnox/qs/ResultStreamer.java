package se.fortnox.qs;

import rx.Observable;
import rx.Observer;
import rx.functions.Action2;
import rx.observables.AsyncOnSubscribe;
import rx.schedulers.Schedulers;
import se.fortnox.reactivewizard.db.ConnectionProvider;

import javax.inject.Inject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static rx.Observable.just;

public class ResultStreamer {
    private final ConnectionProvider connectionProvider;

    @Inject
    public ResultStreamer(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
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
        return Observable
                .create(AsyncOnSubscribe.createStateless(new QueryExecutor(connectionProvider, bufferSize)))
                .subscribeOn(Schedulers.io())
                .buffer(bufferSize);
    }

    private static class QueryExecutor implements Action2<Long, Observer<Observable<? extends Integer>>> {
        private final ConnectionProvider connectionProvider;
        private final ArrayList<Integer> buffer;
        private final int bufferSize;
        private ResultSet resultSet;

        public QueryExecutor(ConnectionProvider connectionProvider, int bufferSize) {
            this.connectionProvider = connectionProvider;
            this.buffer = new ArrayList<>(bufferSize);
            this.bufferSize = bufferSize;
        }

        @Override
        public void call(Long requested, Observer<Observable<? extends Integer>> observer) {
            if(resultSet == null) {
                try {
                    resultSet = initialize(requested, bufferSize);
                } catch (Exception e) {
                    observer.onError(e);
                    return;
                }
            }
            try {
                buffer.clear();
                while (resultSet.next() && buffer.size() < requested) {
                    buffer.add(resultSet.getInt(1));
                }
                if(buffer.size() > 0 ) {
                    observer.onNext(Observable.from(buffer));
                }
                if(buffer.size() < requested) {
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

        private ResultSet initialize(Long requested, int bufferSize) throws SQLException {
            Connection connection = connectionProvider.get();
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT nbr FROM large ORDER BY nbr LIMIT 6000", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            preparedStatement.setFetchSize(bufferSize);
            preparedStatement.closeOnCompletion();

            return preparedStatement.executeQuery();
        }
    }
}
