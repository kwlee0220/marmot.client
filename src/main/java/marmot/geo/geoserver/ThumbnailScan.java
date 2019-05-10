package marmot.geo.geoserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.InsufficientThumbnailException;
import marmot.RecordSet;
import utils.LoggerSettable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ThumbnailScan implements LoggerSettable {
	private final DataSet m_ds;
	private Envelope m_range;
	private int m_sampleCount;
	private Logger m_logger;
	
	static ThumbnailScan on(DataSet ds, Envelope range, long sampleCount) {
		return new ThumbnailScan(ds, range, sampleCount);
	}
	
	static FOption<RecordSet> scan(DataSet ds, Envelope range, long sampleCount) {
		try {
			RecordSet rset = ThumbnailScan.on(ds, range, sampleCount).run();
			return FOption.of(RecordSet.from(rset.toList()));
		}
		catch ( InsufficientThumbnailException e ) {
			return FOption.empty();
		}
	}
	
	private ThumbnailScan(DataSet ds, Envelope range, long sampleCount) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		
		m_ds = ds;
		m_range = range;
		m_sampleCount = (int)sampleCount;
		
		m_logger = LoggerFactory.getLogger(ThumbnailScan.class);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	public RecordSet run() {
		return m_ds.readThumbnail(m_range, m_sampleCount);
	}
}
