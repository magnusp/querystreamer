package se.fortnox.qs;

import rx.Observable;
import se.fortnox.reactivewizard.jaxrs.Stream;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.concurrent.TimeUnit;

import static rx.Observable.just;

@Singleton
@Path("/")
public class DataResource {
    private final Provider<ResultStreamer> resultStreamerProvider;

    @Inject
    public DataResource(Provider<ResultStreamer> resultStreamerProvider) {
        this.resultStreamerProvider = resultStreamerProvider;
    }

    @Stream
    @Path("/data")
    @GET
    public Observable<Holder> streamData() {
        ResultStreamer resultStreamer = resultStreamerProvider.get();
        return resultStreamer.emission(853).concatMap(value -> {
            return just(new Holder(value));
        });
    }

    @Stream
    @Path("/data-delayed")
    @GET
    public Observable<Holder> streamDataDelayed() {
        ResultStreamer resultStreamer = resultStreamerProvider.get();
        return resultStreamer.delayedElementEmission(853, 50, TimeUnit.MICROSECONDS).concatMap(value -> {
            return just(new Holder(value));
        });
    }
}
