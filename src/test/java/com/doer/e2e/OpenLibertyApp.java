package com.doer.e2e;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Open Liberty: {@code ROOT.war}, {@code server.xml} and the Postgres driver mounted to {@code /config}. A
 * data source of {@code server.xml} cannot use the driver of the WAR, so the driver is also a file next to it.
 */
class OpenLibertyApp extends JakartaEeServerApp {
    static final String OPEN_LIBERTY_IMAGE = "icr.io/appcafe/open-liberty:26.0.0.10-full-java25-openj9-ubi-minimal";

    @Override
    String artifactId() {
        return "transitsims-open-liberty";
    }

    @Override
    String dataSourceJndiName() {
        return "jdbc/transitsims";
    }

    /** Copies the Postgres driver to {@code target/liberty/postgresql.jar}. */
    @Override
    String extraPlugins() {
        return """
                            <plugin>
                                <artifactId>maven-dependency-plugin</artifactId>
                                <version>3.9.0</version>
                                <executions>
                                    <execution>
                                        <id>copy-postgresql-driver</id>
                                        <phase>package</phase>
                                        <goals>
                                            <goal>copy</goal>
                                        </goals>
                                        <configuration>
                                            <artifactItems>
                                                <artifactItem>
                                                    <groupId>org.postgresql</groupId>
                                                    <artifactId>postgresql</artifactId>
                                                    <version>${postgresql.version}</version>
                                                    <destFileName>postgresql.jar</destFileName>
                                                </artifactItem>
                                            </artifactItems>
                                            <outputDirectory>${project.build.directory}/liberty</outputDirectory>
                                        </configuration>
                                    </execution>
                                </executions>
                            </plugin>
                """;
    }

    @Override
    void configureServer(Path workdir) throws IOException {
        // Flyway finds a classpath location in WebSphere and Open Liberty by this empty file in it
        writeFile(workdir, "src/main/resources/db/migration/flyway.location", "");
        writeFile(workdir, "src/main/liberty/server.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <server>
                    <featureManager>
                        <feature>webProfile-11.0</feature>
                        <feature>jdbc-4.3</feature>
                    </featureManager>

                    <!-- The defaults are for a run from the IDE (doer.e2e.runtime=external) -->
                    <variable name="env.E2E_DB_URL" defaultValue="jdbc:postgresql://localhost:5432/doer"/>
                    <variable name="env.E2E_DB_USER" defaultValue="doer"/>
                    <variable name="env.E2E_DB_PASSWORD" defaultValue="doer"/>

                    <!-- The INFO messages of the application on the console, as in the other runtimes -->
                    <logging consoleLogLevel="INFO"/>

                    <httpEndpoint id="defaultHttpEndpoint" host="*" httpPort="8080" httpsPort="-1"/>

                    <library id="postgresql">
                        <file name="${server.config.dir}/postgresql.jar"/>
                    </library>
                    <dataSource id="transitsims" jndiName="%s">
                        <jdbcDriver libraryRef="postgresql"/>
                        <properties.postgresql URL="${env.E2E_DB_URL}" user="${env.E2E_DB_USER}" password="${env.E2E_DB_PASSWORD}"/>
                        <connectionManager maxPoolSize="4"/>
                    </dataSource>

                    <!-- Otherwise the context root is the name of the WAR -->
                    <webApplication location="ROOT.war" contextRoot="/"/>
                </server>
                """.formatted(dataSourceJndiName()));
    }

    /** The command of the image: {@code server run defaultServer}. */
    @Override
    public DockerRun dockerRun(Path workdir) {
        String config = "/opt/ol/wlp/usr/servers/defaultServer/";
        return new DockerRun(List.of(
                "-v", workdir.resolve("src/main/liberty/server.xml") + ":" + config + "server.xml:ro",
                "-v", workdir.resolve("target/liberty/postgresql.jar") + ":" + config + "postgresql.jar:ro",
                "-v", workdir.resolve("target/ROOT.war") + ":" + config + "apps/ROOT.war:ro"),
                OPEN_LIBERTY_IMAGE, List.of());
    }
}
