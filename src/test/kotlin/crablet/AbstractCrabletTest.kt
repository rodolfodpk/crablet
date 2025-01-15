package crablet

import io.vertx.core.Vertx
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.MountableFile

abstract class AbstractCrabletTest {
    protected val vertx: Vertx = Vertx.vertx()

    protected val pool: Pool by lazy {
        postgresqlContainer.start()
        val connectOptions =
            PgConnectOptions()
                .setPort(if (postgresqlContainer.isRunning) postgresqlContainer.firstMappedPort else 5432)
                .setHost("127.0.0.1")
                .setDatabase(DB_NAME)
                .setUser(DB_USERNAME)
                .setPassword(DB_PASSWORD)
        val poolOptions = PoolOptions().setMaxSize(5)
        Pool.pool(vertx, connectOptions, poolOptions)
    }

    protected val testRepository: TestRepository by lazy {
        TestRepository(pool)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractCrabletTest::class.java)

        val PG_DOCKER_IMAGE = "postgres:latest"
        val DB_NAME = "postgres"
        val DB_USERNAME = "postgres"
        val DB_PASSWORD = "postgres"

        val postgresqlContainer =
            PostgreSQLContainer<Nothing>(PG_DOCKER_IMAGE)
                .apply {
                    withDatabaseName(DB_NAME)
                    withUsername(DB_USERNAME)
                    withPassword(DB_PASSWORD)
                    withCopyFileToContainer(
                        MountableFile.forClasspathResource("ddl/1-events_table.sql"),
                        "/docker-entrypoint-initdb.d/1-events_table.sql",
                    )
                    withCopyFileToContainer(
                        MountableFile.forClasspathResource("ddl/2-append_events.sql"),
                        "/docker-entrypoint-initdb.d/2-append_events.sql",
                    )
                    withCopyFileToContainer(
                        MountableFile.forClasspathResource("ddl/3-subscriptions_table.sql"),
                        "/docker-entrypoint-initdb.d/3-subscriptions_table.sql",
                    )
                    withCopyFileToContainer(
                        MountableFile.forClasspathResource("ddl/4-accounts_view.sql"),
                        "/docker-entrypoint-initdb.d/4-accounts_view.sql.sql",
                    )
                    withEnv("POSTGRES_LOG_CONNECTIONS", "on")
                    withEnv("POSTGRES_LOG_DISCONNECTIONS", "on")
                    withEnv("POSTGRES_LOG_DURATION", "on")
                    withEnv("POSTGRES_LOG_STATEMENT", "all")
                    withEnv("POSTGRES_LOG_DIRECTORY", "/var/log/pg_log")
                    withLogConsumer { frame -> println(frame.utf8String) }
                }
    }
}
