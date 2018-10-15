package marmot.geoserver.plugin;

import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleResult {
	private final RecordSet m_result;
	private final boolean m_isSampled;
	
	public static SampleResult all(RecordSet result) {
		return new SampleResult(result, false);
	}
	
	public static SampleResult sampled(RecordSet result) {
		return new SampleResult(result, true);
	}
	
	private SampleResult(RecordSet result, boolean isSampled) {
		m_result = result;
		m_isSampled = isSampled;
	}
	
	public RecordSet getResult() {
		return m_result;
	}
	
	public boolean isSampled() {
		return m_isSampled;
	}
}