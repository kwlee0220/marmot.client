package marmot.support;

import java.util.function.Supplier;

import marmot.MarmotRuntime;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetBackedRecordSetSupplier implements Supplier<RecordSet> {
	private final MarmotRuntime m_marmot;
	private final String m_dsId;
	
	public DataSetBackedRecordSetSupplier(MarmotRuntime marmot, String dsId) {
		m_marmot = marmot;
		m_dsId = dsId;
	}
	
	@Override
	public RecordSet get() {
		return m_marmot.getDataSet(m_dsId).read();
	}
	
	public void delete() {
		m_marmot.deleteDataSet(m_dsId);
	}
}
