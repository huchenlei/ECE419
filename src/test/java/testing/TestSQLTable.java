package testing;

import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import server.KVIterateStore;
import server.sql.SQLIterateTable;
import server.sql.SQLTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSQLTable extends TestCase {
    public static SQLTable st;

    public void test01Create() {
        Map<String, Class> meta = new HashMap<>();
        meta.put("age", Double.class);
        meta.put("name", String.class);
        meta.put("weight", Double.class);

        st = new SQLIterateTable("student",
                new KVIterateStore("student_table"), meta);
    }

    public void test02Insert() throws IOException {
        for (int i = 0; i < 10; i++) {
            Map<String, Object> student = new HashMap<>();
            student.put("age", (double) i);
            student.put("name", "student" + i);
            student.put("weight", (double) i);

            st.insert(student);
        }
    }

    public void test03Query() throws IOException {
        List<Map<String, Object>> result =
                st.query(Arrays.asList("age", "name", "weight"),
                        row -> (Double) row.get("age") > 2);

        assertEquals(7, result.size());
        result.forEach(r -> assertEquals(3, r.size()));

        result = st.query(Arrays.asList("age", "weight"),
                row -> row.get("name").equals("student1"));
        assertEquals(1, result.size());
        assertEquals(1d, result.get(0).get("weight"));
        assertEquals(1d, result.get(0).get("age"));
    }
}
