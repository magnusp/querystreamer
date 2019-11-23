package se.fortnox.qs;

import rx.Observable;
import se.fortnox.reactivewizard.jaxrs.Stream;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static rx.Observable.just;

@Singleton
@Path("/")
public class DataResource {
    private static final String QUERY = "SELECT nbr FROM large ORDER BY nbr LIMIT 1000000";
    private static final int DEFAULT_BUFFER_SIZE = 999;
    private final Provider<ResultStreamer> resultStreamerProvider;

    @Inject
    public DataResource(Provider<ResultStreamer> resultStreamerProvider) {
        this.resultStreamerProvider = resultStreamerProvider;
    }

    @Stream
    @Path("/data")
    @GET
    public Observable<Holder> streamData(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer resultStreamer = resultStreamerProvider.get();
        return resultStreamer
                .query(QUERY)
                .emission(bufferSize)
                .concatMap(value -> just(new Holder(value)));
    }

    @Stream
    @Path("/data-delayed")
    @GET
    public Observable<Holder> streamDataDelayed(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer resultStreamer = resultStreamerProvider.get();
        return resultStreamer
                .query("SELECT nbr FROM large ORDER BY nbr LIMIT 1000000")
                .delayedElementEmission(bufferSize, 50, TimeUnit.MICROSECONDS)
                .concatMap(value -> just(new Holder(value)));
    }

    @Path("/no-stream")
    @GET
    public Observable<List<Holder>> nonStreamedData(@QueryParam("buffersize") Integer bufferSize) {
        if(bufferSize == null) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer resultStreamer = resultStreamerProvider.get();
        return resultStreamer
                .query("SELECT nbr FROM large ORDER BY nbr LIMIT 1000000")
                .emission(bufferSize)
                .concatMap(value -> just(new Holder(value)))
                .toList();
    }
}
