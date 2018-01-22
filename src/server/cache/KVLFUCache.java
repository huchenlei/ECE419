package server.cache;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KVLFUCache extends AbstractKVCache {
	
	private HashMap<String,Integer> LFUMap;
	
	public KVLFUCache(Integer cacheSize) {
		super(cacheSize);
		this.cacheMap = new HashMap<String, String>(this.getCacheSize());
		this.LFUMap = new HashMap<String,Integer>(this.getCacheSize()); 
    }
	
	//apply this function before get and put for LFU strategy
	public void LFUReplace(String key) {
		int counter;
		if (this.getCacheSize() == 0) {
			return;
		}
		else if (this.cacheMap.containsKey(key)) {
			//increment the counter of the key
			counter = LFUMap.get(key);
			counter += 1;
			LFUMap.put(key, counter);
			return;
		}
		else if (this.cacheMap.size() >= this.getCacheSize()) {
		//remove the lfu entry
			String keyToRemove;
			Iterator itr = LFUMap.entrySet().iterator(); 
			Map.Entry entry;
			entry = (Map.Entry)itr.next();
			counter = (int)entry.getValue();
			keyToRemove = (String)entry.getKey();
			while (itr.hasNext()) {
				entry = (Map.Entry)itr.next();
				if ((int)entry.getValue() < counter) {
					counter = (int)entry.getValue();
					keyToRemove = (String)entry.getKey();
				}	
			}
			this.cacheMap.remove(keyToRemove);
			LFUMap.remove(keyToRemove);
		}
		//record the counter of the new entry
		//cache map entry is not inserted in this function
		counter = 1;
		LFUMap.put(key, counter);
		return;
	}

    @Override
    public void put(String key, String value) {
	    LFUReplace(key);
        super.put(key, value);
    }

    @Override
    public String get(String key) {
        LFUReplace(key);
        return super.get(key);
    }
}
