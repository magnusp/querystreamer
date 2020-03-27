package se.fortnox.qs;

import com.google.inject.Injector;
import com.google.inject.Provider;
import liquibase.exception.LiquibaseException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainerProvider;
import se.fortnox.reactivewizard.config.TestInjector;
import se.fortnox.reactivewizard.db.ConnectionProvider;
import se.fortnox.reactivewizard.db.config.DatabaseConfig;
import se.fortnox.reactivewizard.dbmigrate.LiquibaseConfig;
import se.fortnox.reactivewizard.dbmigrate.LiquibaseMigrate;
import se.fortnox.reactivewizard.server.ServerConfig;

import java.sql.SQLException;

public class IntegrationTest {
    private static final String QUERY = "SELECT nbr FROM large ORDER BY nbr";
    private static Injector injector;
    private static Provider<ResultStreamerBuilder> resultStreamerBuilderProvider;
    private static ConnectionProvider connectionProvider;

    @BeforeClass
    static public void beforeClass() throws LiquibaseException {
        JdbcDatabaseContainer jdbcDatabaseContainer = new PostgreSQLContainerProvider().newInstance();
        jdbcDatabaseContainer.start();
        injector = TestInjector
                .create(binder -> {
                    binder.bind(ServerConfig.class).toInstance(new ServerConfig(){{
                        setEnabled(false);
                    }});
                    binder.bind(DatabaseConfig.class).toInstance(new DatabaseConfig() {{
                        setUrl(jdbcDatabaseContainer.getJdbcUrl());
                        setUser(jdbcDatabaseContainer.getUsername());
                        setPassword(jdbcDatabaseContainer.getPassword());
                    }});
                    binder.bind(LiquibaseConfig.class).toInstance(new LiquibaseConfig() {{
                        setUrl(jdbcDatabaseContainer.getJdbcUrl());
                        setUser(jdbcDatabaseContainer.getUsername());
                        setPassword(jdbcDatabaseContainer.getPassword());
                    }});
                });
        injector.getInstance(LiquibaseMigrate.class).run();
        connectionProvider = injector.getInstance(ConnectionProvider.class);
    }

    @Test
    public void testEmitsExpectedAmount() {
        ResultStreamerBuilder<Holder> rsb = new ResultStreamerBuilder<>(connectionProvider);
        ResultStreamer<Holder> resultStreamer = rsb
                .query(QUERY)
                .mappedBy(resultSet -> {
                    try {
                        return new Holder(resultSet.getInt(1));
                    } catch (SQLException e) {
                        return new Holder(-1);
                    }
                })
                .build();
        resultStreamer
                .emit(500_000)
                .test()
                .awaitTerminalEvent()
                .assertValueCount(1_000_000);
    }
}