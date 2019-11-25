package se.fortnox.qs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import se.fortnox.reactivewizard.jaxrs.Stream;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static rx.Observable.just;

@Singleton
@Path("/")
public class DataResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);
    private static final String QUERY = "SELECT nbr FROM large ORDER BY nbr LIMIT 1000000";
    private static final int DEFAULT_BUFFER_SIZE = 999;
    private final Provider<ResultStreamer<Integer>> resultStreamerProvider;

    @Inject
    public DataResource(Provider<ResultStreamer<Integer>> resultStreamerProvider) {
        this.resultStreamerProvider = resultStreamerProvider;
    }

    @Stream
    @Path("/data")
    @GET
    public Observable<List<Holder>> streamData(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer<Integer> resultStreamer = resultStreamerProvider.get();
        return resultStreamer
                .query(QUERY, (preparedStatement) -> {}, (resultSet) -> {
                    try {
                        return resultSet.getInt(1);
                    } catch (SQLException e) {
                        return -1;
                    }
                })
                .emission(bufferSize)
                .concatMap(value -> just(new Holder(value)))
            .buffer(100);
    }

    @Stream
    @Path("/data-delayed")
    @GET
    public Observable<Holder> streamDataDelayed(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer<Integer> resultStreamer = resultStreamerProvider.get();
        return resultStreamer
            .query(QUERY, (preparedStatement) -> {}, (resultSet) -> {
                try {
                    return resultSet.getInt(1);
                } catch (SQLException e) {
                    return -1;
                }
            })
                .delayedElementEmission(bufferSize, 50, TimeUnit.MICROSECONDS)
                .concatMap(value -> just(new Holder(value)));
    }

    @Path("/no-stream")
    @GET
    public Observable<List<Holder>> nonStreamedData(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer<Integer> resultStreamer = resultStreamerProvider.get();
        return resultStreamer
            .query(QUERY, (preparedStatement) -> {}, (resultSet) -> {
                try {
                    return resultSet.getInt(1);
                } catch (SQLException e) {
                    return -1;
                }
            })
                .emission(bufferSize)
                .concatMap(value -> just(new Holder(value)))
                .toList();
    }
}
