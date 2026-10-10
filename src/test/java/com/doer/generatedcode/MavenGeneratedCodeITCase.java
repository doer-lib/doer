package com.doer.generatedcode;

import static com.doer.testkit.Sources.TEST_DOER_SERVICE;
import static org.junit.jupiter.api.io.CleanupMode.NEVER;

import com.doer.testkit.InWorkspace;
import com.doer.testkit.Toolchain;
import com.doer.testkit.Workspaces;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

/** {@link GeneratedCodeTest} on CarWash built with Maven, run in its own JVM. */
@TestInstance(Lifecycle.PER_CLASS)
public class MavenGeneratedCodeITCase implements GeneratedCodeTest, InWorkspace {

    // Static: JUnit sets static fields before @BeforeAll, instance fields only before each test
    @TempDir(factory = Workspaces.class, cleanup = NEVER)
    static Path workspace;
    CarWashProcess carWash;

    @BeforeAll
    void build() throws Exception {
        copySources("e2e/carwash", "src/main/java/carwash");
        writeSource("src/main/java/demo/test/TestDoerService.java", TEST_DOER_SERVICE);
        writeSource("src/main/java/carwash/Main.java", CarWashProcess.MAIN);
        writeSource("pom.xml", POM.formatted(Toolchain.doerLibVersion, Toolchain.jakartaVersion,
                Toolchain.parssonVersion));
        mvn("mvn-package", "package").assertStatus(0);
        carWash = CarWashProcess.start(workspace, Toolchain.runClasspath("target/classes"));
    }

    @AfterAll
    void stop() throws Exception {
        if (carWash != null) {
            carWash.close();
        }
    }

    @Override
    public Path getWorkspace() {
        return workspace;
    }

    @Override
    public String callTaskRunner(String request) throws Exception {
        return carWash.runTask(request);
    }

    /** Project of CarWash; parameters: doer (library and processor), Jakarta EE API and Parsson versions. */
    static final String POM = """
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
              <modelVersion>4.0.0</modelVersion>
              <groupId>carwash</groupId>
              <artifactId>carwash</artifactId>
              <version>1.0-SNAPSHOT</version>

              <properties>
                <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
                <maven.compiler.release>17</maven.compiler.release>
              </properties>

              <dependencies>
                <dependency>
                  <groupId>com.java-doer</groupId>
                  <artifactId>doer</artifactId>
                  <version>%1$s</version>
                </dependency>
                <dependency>
                  <groupId>jakarta.platform</groupId>
                  <artifactId>jakarta.jakartaee-api</artifactId>
                  <version>%2$s</version>
                </dependency>
                <dependency>
                  <groupId>org.eclipse.parsson</groupId>
                  <artifactId>parsson</artifactId>
                  <version>%3$s</version>
                  <scope>runtime</scope>
                </dependency>
              </dependencies>

              <build>
                <plugins>
                  <plugin>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.16.0</version>
                    <configuration>
                      <parameters>true</parameters>
                      <annotationProcessorPaths>
                        <path>
                          <groupId>com.java-doer</groupId>
                          <artifactId>doer</artifactId>
                          <version>%1$s</version>
                        </path>
                      </annotationProcessorPaths>
                    </configuration>
                  </plugin>
                </plugins>
              </build>
            </project>
            """;
}
