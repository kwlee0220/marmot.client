package marmot.geo.geoserver;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.InsufficientThumbnailException;
import marmot.RecordSet;
import utils.LoggerSettable;
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
		Objects.requireNonNull(ds, "DataSet");
		
		m_ds = ds;
		m_range = range;
		m_sampleCount = (int)sampleCount;
		
		m_logger = LoggerFactory.getLogger(FullScan.class);
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
		String msg = String.format("thumbnail-scan: dataset=%s, nsamples=%d",
									m_ds, m_sampleCount);
		getLogger().info(msg);
		
		return m_ds.readThumbnail(m_range, m_sampleCount);
	}
}
