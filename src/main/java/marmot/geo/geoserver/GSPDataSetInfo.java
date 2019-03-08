package marmot.geo.geoserver;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.optor.AggregateFunction;
import utils.Guard;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class GSPDataSetInfo {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPDataSetInfo.class);
	
	private final DataSet m_ds;
	private Guard m_guard = Guard.create();
	private long m_count = -1;
	private Envelope m_bounds = null;
	
	GSPDataSetInfo(DataSet ds) {
		m_ds = ds;
		
		loadStatistics();
	}
	
	MarmotRuntime getMarmotRuntime() {
		return m_ds.getMarmotRuntime();
	}
	
	DataSet getDataSet() {
		return m_ds;
	}
	
	RecordSchema getRecordSchema() {
		return m_ds.getRecordSchema();
	}
	
	GeometryColumnInfo getGeometryColumnInfo() {
		return m_ds.getGeometryColumnInfo();
	}
	
	boolean hasSpatialIndex() {
		return m_ds.isSpatiallyClustered();
	}
	
	long getRecordCount() {
		try {
			return m_guard.awaitUntilAndGet(() -> m_count >= 0, () -> m_count);
		}
		catch ( InterruptedException e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	Envelope getBounds() {
		try {
			return m_guard.awaitUntilAndGet(() -> m_bounds != null, () -> m_bounds);
		}
		catch ( InterruptedException e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private void loadStatistics() {
		if ( m_ds.getType() == DataSetType.FILE ) {
			m_count = m_ds.getRecordCount();
			m_bounds = m_ds.getBounds();
		}
		else if ( m_ds.getType() == DataSetType.TEXT ) {
			if ( m_ds.hasGeometryColumn() ) {
				SpatialIndexInfo idxInfo = m_ds.getDefaultSpatialIndexInfo().get();
				m_bounds = idxInfo.getDataBounds();
				m_count = idxInfo.getRecordCount();
			}
			else {
				CompletableFuture.runAsync(() -> {
					m_guard.run(this::aggregate, true);
				});
			}
		}
	}
	
	private void aggregate() {
		MarmotRuntime marmot = m_ds.getMarmotRuntime();
		
		s_logger.info("aggregating: dataset[{}] count and mbr......", m_ds.getId());
		
		GeometryColumnInfo gcInfo = m_ds.getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("aggregate")
							.load(m_ds.getId())
							.aggregate(AggregateFunction.COUNT(),
										AggregateFunction.ENVELOPE(gcInfo.name()))
							.build();
		Record result = marmot.executeToRecord(plan).get();
		m_count = result.getLong(0);
		m_bounds = ((Polygon)result.get(1)).getEnvelopeInternal();
		
		s_logger.info("aggregated: count={},  mbr={}", m_count, m_bounds);
	}
}