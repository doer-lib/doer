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

/** {@link GeneratedCodeTest} on CarWash compiled with javac, run in its own JVM. */
@TestInstance(Lifecycle.PER_CLASS)
public class JavacGeneratedCodeITCase implements GeneratedCodeTest, InWorkspace {

    // Static: JUnit sets static fields before @BeforeAll, instance fields only before each test
    @TempDir(factory = Workspaces.class, cleanup = NEVER)
    static Path workspace;
    CarWashProcess carWash;

    @BeforeAll
    void build() throws Exception {
        copySources("e2e/carwash", "carwash");
        writeSource("TestDoerService.java", TEST_DOER_SERVICE);
        writeSource("carwash/Main.java", CarWashProcess.MAIN);
        javac(javaFiles()).assertStatus(0);
        carWash = CarWashProcess.start(workspace, Toolchain.runClasspath("classes"));
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
}
