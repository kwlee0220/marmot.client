package marmot.geoserver.plugin;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.geo.CRSUtils;
import marmot.geo.geotools.GeoToolsUtils;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class GSPFeatureSourceInfo {
	static final long CACHEABLE_SIZE = UnitUtils.parseByteSize("512mb");
	
	final DataSet m_ds;
	final SimpleFeatureType m_sfType;
	final GeometryColumnInfo m_geomInfo;
	final String m_srid;
	final CoordinateReferenceSystem m_crs;
	final BoundingBox m_bbox;
	final boolean m_cacheable;

	private final Lock m_lock = new ReentrantLock();
	private final Condition m_cond = m_lock.newCondition();
	final Guard m_guard = Guard.by(m_lock, m_cond);
	@GuardedBy("m_lock") BoundingBox m_tileBox;
	@GuardedBy("m_lock") ListFeatureCollection m_cached;	// nullable
	
	GSPFeatureSourceInfo(DataSet ds) {
		m_ds = ds;

		String sfTypeName = GSPUtils.toSimpleFeatureTypeName(ds.getId());
		m_sfType = GeoToolsUtils.toSimpleFeatureType(sfTypeName, ds);
		m_geomInfo = m_ds.getGeometryColumnInfo();
		m_srid = m_geomInfo.srid();
		m_crs = CRSUtils.toCRS(m_geomInfo.srid());
		
		Envelope envl = m_ds.getBounds();
		m_bbox = new ReferencedEnvelope(envl.getMinX(), envl.getMaxX(), envl.getMinY(),
										envl.getMaxY(), m_crs);
		
		m_cacheable = m_ds.length() <= CACHEABLE_SIZE;
	}
	
	String getDataSetId() {
		return m_ds.getId();
	}
	
	DataSet getDataSet() {
		return m_ds;
	}
}
