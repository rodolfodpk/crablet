package crablet.postgres

import io.vertx.core.Future
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.MountableFile

abstract class AbstractCrabletTest {

    companion object {

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
                        MountableFile.forClasspathResource("./ddl/1-events_table.sql"),
                        "/docker-entrypoint-initdb.d/1-events_table.sql",
                    )
                    withCopyFileToContainer(
                        MountableFile.forClasspathResource("./ddl/2-append_events.sql"),
                        "/docker-entrypoint-initdb.d/2-append_events.sql",
                    )
                    withEnv("POSTGRES_LOG_CONNECTIONS", "on")
                    withEnv("POSTGRES_LOG_DISCONNECTIONS", "on")
                    withEnv("POSTGRES_LOG_DURATION", "on")
                    withEnv("POSTGRES_LOG_STATEMENT", "all")
                    withEnv("POSTGRES_LOG_DIRECTORY", "/var/log/pg_log")
                    withLogConsumer { frame -> println(frame.utf8String) }
                }

        val pool: Pool by lazy {
            postgresqlContainer.start()
            val connectOptions = PgConnectOptions()
                .setPort(if (postgresqlContainer.isRunning) postgresqlContainer.firstMappedPort else 5432)
                .setHost("127.0.0.1")
                .setDatabase(DB_NAME)
                .setUser(DB_USERNAME)
                .setPassword(DB_PASSWORD)
            val poolOptions = PoolOptions().setMaxSize(5)
            Pool.pool(connectOptions, poolOptions)
        }

        fun cleanDatabase(): Future<Void> {
            return pool.query("TRUNCATE TABLE events").execute()
                .compose {
                    pool.query("ALTER SEQUENCE events_sequence_id_seq RESTART WITH 1").execute()
                }.mapEmpty()
        }

        fun dumpEvents(): Future<Void> {
            return pool.query("select * from events order by sequence_id").execute()
                .onSuccess { rs ->
                    println("Events -------------")
                    rs.forEach {
                        println(it.toJson())
                    }
                }
                .mapEmpty()
        }
    }

}