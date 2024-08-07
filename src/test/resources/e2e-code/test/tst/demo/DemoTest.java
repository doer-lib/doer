package tst.demo;


import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.doer.AcceptStatus;
import com.doer.Task;

/**
 * This test class is needed to check that DoerProcessor will skip code generation during
 * test compilation (code already was generated, when main code compiled).
 */
public class DemoTest {
    @Test
    void demoTest() {
        assertTrue(1 == 1);
    }

    @AcceptStatus("demo")
    public void x(Task task) {
        task.setStatus(null);
    }
}
