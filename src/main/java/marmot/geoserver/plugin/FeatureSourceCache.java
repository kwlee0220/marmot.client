package marmot.geoserver.plugin;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FeatureSourceCache extends LinkedHashMap<String, GSPFeatureSource> {
	private static final long serialVersionUID = -2694176719194229909L;
	private static final int CACHE_SIZE = 2;
	
	private long m_maxSize;
	
	FeatureSourceCache() {
		super(CACHE_SIZE*2, 0.75f, true);
	}
	
	void setCacheSize(long size) {
		m_maxSize = size;
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<String,GSPFeatureSource> eldest) {
		if ( size() >= CACHE_SIZE ) {
			System.out.printf("victim selected: dataset=%s cache_size=%d%n",
								eldest.getValue().getName(), size());
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("MarmotFeatureSourceCache[%d/%d]",
							size(), m_maxSize);
	}

}
