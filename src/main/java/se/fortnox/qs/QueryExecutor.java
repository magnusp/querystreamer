package se.fortnox.qs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action2;
import se.fortnox.reactivewizard.db.ConnectionProvider;

import java.sql.*;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

class QueryExecutor<R> implements Action2<Long, Observer<Observable<? extends R>>> {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);
    private final ConnectionProvider connectionProvider;
    private final String                      query;
    private final Function<ResultSet, ? extends R> resultMapper;
    private final int fetchSize;
    private ResultSet                         resultSet;
    private Consumer<PreparedStatement> parameterBinder;

    QueryExecutor(ConnectionProvider connectionProvider,
                  String query,
                  Consumer<PreparedStatement> parameterBinder,
                  Function<ResultSet, ? extends R> resultMapper,
                  int fetchSize
    ) {
        this.connectionProvider = connectionProvider;
        this.query = query;
        this.parameterBinder = parameterBinder;
        this.resultMapper = resultMapper;
        this.fetchSize = fetchSize;
    }

    @Override
    public void call(Long requested, Observer<Observable<? extends R>> observer) {
        if (resultSet == null) {
            try {
                resultSet = initialize(fetchSize);
            } catch (Exception e) {
                observer.onError(e);
                return;
            }
        }
        try {
            int toProduce;
            if(requested > fetchSize) {
                LOG.info("Requested {} with fetchSize {}, throttle to fetchSize", requested, fetchSize);
                toProduce = fetchSize;
            } else {
                LOG.info("Requested {} with fetchSize {}, throttle to request.", requested, fetchSize);
                toProduce = requested.intValue();
            }

            R[] values = (R[])new Object[toProduce];
            boolean gotNext;
            int counter = -1;
            while ((gotNext = resultSet.next()) && counter < toProduce) { // TODO Make this actually work
                if(counter == -1) {
                    counter = 0;
                }
                values[counter] = resultMapper.apply(resultSet);
                counter = counter + 1;
            }
            LOG.info("Counter reached {}", counter);
            if (counter > 0) {
                LOG.info("Emitting {}", counter);
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

        PreparedStatement preparedStatement = connection.prepareStatement(
                query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
        );
        parameterBinder.accept(preparedStatement);
        preparedStatement.setFetchSize(bufferSize);
        preparedStatement.closeOnCompletion();

        return preparedStatement.executeQuery();
    }

    void abort() throws SQLException {
        LOG.info("Stopping due to upstream unsubscribed");
        dispose(resultSet);
    }
}
