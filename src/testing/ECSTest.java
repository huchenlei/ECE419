package testing;

import ecs.ECS;
import junit.framework.TestCase;

import java.io.IOException;

public class ECSTest extends TestCase {
    private ECS ecs = null;

    public void testECSConfig() throws IOException {
        ecs = new ECS("./ecs.config");
    }
}
