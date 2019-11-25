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

@Singleton
@Path("/")
public class DataResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);
    private static final String QUERY = "SELECT nbr FROM large ORDER BY nbr LIMIT 19 OFFSET 999981";
    private static final int DEFAULT_BUFFER_SIZE = 999;
    private final Provider<ResultStreamerBuilder<Holder>> resultStreamerBuilderProvider;

    @Inject
    public DataResource(Provider<ResultStreamerBuilder<Holder>> resultStreamerBuilderProvider) {
        this.resultStreamerBuilderProvider = resultStreamerBuilderProvider;
    }

    @Stream
    @Path("/streamed-data")
    @GET
    public Observable<Holder> streamedData(@QueryParam("fetchsize") Integer fetchSize) {
        if(fetchSize == null) {
            fetchSize = DEFAULT_BUFFER_SIZE;
        }
        ResultStreamer<Holder> resultStreamer = resultStreamerBuilderProvider
                .get()
                .query(QUERY)
                .mappedBy(resultSet -> {
                    try {
                        return new Holder(resultSet.getInt(1));
                    } catch (SQLException e) {
                        return new Holder(-1);
                    }
                })
                .build();
        return resultStreamer
                .emit(fetchSize)
                .doOnError(x -> System.out.println(x));
    }

    @Path("/data")
    @GET
    public Observable<List<Holder>> nonStreamedData(@QueryParam("fetchsize") Integer fetchSize) {
        return streamedData(fetchSize)
                .toList();
    }
}
