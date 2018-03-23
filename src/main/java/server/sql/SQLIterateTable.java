package server.sql;

import app_kvServer.KVServer;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import ecs.ECSNode;
import server.KVIterateStore;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLIterateTable implements SQLTable {
    private static final String TABLE_COL_ID = "_table";
    private static final String PRIMARY_KEY = "_pk";
    public static Gson gson;

    static {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Class.class, new ClassAdapter());
        builder.setExclusionStrategies(new SQLIterateTableStrategy());
        gson = builder.create();
    }

    private static Type type = new TypeToken<Map<String, Object>>() {
    }.getType();

    private String name;
    private KVIterateStore store;
    public Map<String, Class> typeMap;

    public String getName() {
        return name;
    }

    public SQLIterateTable() {
    }

    public void setStore(KVIterateStore store) {
        this.store = store;
    }

    public SQLIterateTable(String name, KVIterateStore store, Map<String, Class> typeMap) {
        this.name = name;
        this.store = store;
        this.typeMap = typeMap;
        this.typeMap.put(TABLE_COL_ID, String.class);
        // No longer use primary key system to identify different object
        // with same values
//        this.typeMap.put(PRIMARY_KEY, Double.class);
    }

    private BiPredicate<String, String> tableSelectWrapper(Predicate<Map<String, Object>> condition) {
        return (key, val) -> {
            // Distinguish with kv store rows
            if (key.length() <= KVServer.MAX_KEY) return false;
            // Convert value string to Json
            Map<String, Object> row = jsonToMap(val);
            String tableName = (String) row.get(TABLE_COL_ID);
            if (!name.equals(tableName)) return false;

            return condition.test(row);
        };
    }

    private void sanityCheck(Map<String, Object> target) {
        for (Map.Entry<String, Object> entry : target.entrySet()) {
            Class clazz = typeMap.get(entry.getKey());
            if (clazz == null) {
                throw new SQLException("Column " + entry.getKey() + " is not in table " + this.name
                        + typeMap);
            }

            if (!clazz.isInstance(entry.getValue())) {
                throw new SQLException("Column " + entry.getKey() +
                        " is assigned incompatible type! Expected " + clazz.toString() +
                        " but got " + entry.getValue().getClass());
            }
        }
    }

    public static synchronized Map<String, Object> jsonToMap(String json) {
        return gson.fromJson(json, type);
    }

    public static synchronized String mapToJson(Object map) {
        return gson.toJson(map);
    }

    @Override
    public List<Map<String, Object>> query(List<String> selector,
                                           Predicate<Map<String, Object>> condition) throws SQLException, IOException {
        List<Map<String, Object>> result = store.select(tableSelectWrapper(condition)).stream()
                .map(kvEntry -> jsonToMap(kvEntry.getValue()))
                .collect(Collectors.toList());
        for (Map<String, Object> entry : result) {
            sanityCheck(entry);
        }
        return result.stream().map(m -> {
            HashMap<String, Object> ret = new HashMap<>();
            for (Map.Entry<String, Object> e : m.entrySet()) {
                if (selector.contains(e.getKey())) {
                    ret.put(e.getKey(), e.getValue());
                }
            }
            return ret;
        }).collect(Collectors.toList());
    }

    @Override
    public Integer update(Map<String, Object> newValue,
                          Predicate<Map<String, Object>> condition) throws SQLException, IOException {
        sanityCheck(newValue);
        if (newValue.keySet().contains(PRIMARY_KEY) || newValue.keySet().contains(TABLE_COL_ID)) {
            throw new SQLException("Cannot update reserved columns");
        }
        List<KVIterateStore.KVEntry> selected = store.select(tableSelectWrapper(condition));
        Collections.reverse(selected);
        for (KVIterateStore.KVEntry entry : selected) {
            Map<String, Object> row = jsonToMap(entry.getValue());
            for (Map.Entry<String, Object> e : newValue.entrySet()) {
                row.put(e.getKey(), e.getValue());
            }
            entry.setValue(mapToJson(row));
            store.updateEntry(entry);
        }
        return selected.size();
    }

    @Override
    public synchronized void insert(Map<String, Object> value) throws SQLException, IOException {
        sanityCheck(value);
        String key = ECSNode.calcHash(mapToJson(value));
        value.put(TABLE_COL_ID, name);
        String val = mapToJson(value);
        store.appendEntry(new KVIterateStore.KVEntry(key, val));
    }

    @Override
    public Integer delete(Predicate<Map<String, Object>> condition) throws SQLException, IOException {
        List<KVIterateStore.KVEntry> selected = store.select(tableSelectWrapper(condition));
        Collections.reverse(selected);
        for (KVIterateStore.KVEntry kvEntry : selected) {
            store.deleteEntry(kvEntry);
        }
        return selected.size();
    }

    @Override
    public void drop() throws IOException {
        this.delete(m -> true);
    }
}

class ClassAdapter extends TypeAdapter<Class> {
    @Override
    public void write(JsonWriter out, Class value) throws IOException {
        if (value == null) {
            out.nullValue();
            return;
        }
        out.value(value.getName());
    }

    @Override
    public Class read(JsonReader in) throws IOException {
        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        }
        String className = in.nextString();
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}

class SQLIterateTableStrategy implements ExclusionStrategy {
    @Override
    public boolean shouldSkipField(FieldAttributes f) {
        return f.getDeclaredClass().equals(KVIterateStore.class);
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
        return false;
    }
}