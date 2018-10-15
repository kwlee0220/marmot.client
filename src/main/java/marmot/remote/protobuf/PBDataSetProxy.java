package marmot.remote.protobuf;

import java.util.List;
import java.util.Objects;

import com.vividsolutions.jts.geom.Envelope;

import io.vavr.control.Option;
import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.GeometryColumnNotExistsException;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.IndexNotFoundException;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetProxy implements DataSet {
	private final PBDataSetServiceProxy m_service;
	private DataSetInfo m_info;
	
	PBDataSetProxy(PBDataSetServiceProxy service, DataSetInfo info) {
		m_service = service;
		m_info = info;
	}

	@Override
	public PBMarmotClient getMarmotRuntime() {
		return m_service.getMarmotRuntime();
	}

	@Override
	public String getId() {
		return m_info.getId();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_info.getRecordSchema();
	}

	@Override
	public DataSetType getType() {
		return m_info.getType();
	}

	@Override
	public String getDirName() {
		return m_info.getDirName();
	}

	@Override
	public boolean hasGeometryColumn() {
		return m_info.getGeometryColumnInfo().isDefined();
	}

	@Override
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_info.getGeometryColumnInfo()
					.getOrElseThrow(() -> new GeometryColumnNotExistsException());
	}

	@Override
	public Envelope getBounds() {
		if ( hasGeometryColumn() ) {
			return m_info.getBounds();
		}
		else {
			throw new GeometryColumnNotExistsException();
		}
	}

	@Override
	public long getRecordCount() {
		return m_info.getRecordCount();
	}

	@Override
	public String getHdfsPath() {
		return m_info.getFilePath();
	}

	@Override
	public long getBlockSize() {
		return m_info.getBlockSize();
	}

	@Override
	public SpatialIndexInfo getDefaultSpatialIndexInfoOrNull() {
		return null;
	}

	@Override
	public SpatialIndexInfo getDefaultSpatialIndexInfo()
		throws GeometryColumnNotExistsException, IndexNotFoundException {
		return null;
	}

	@Override
	public boolean getCompression() {
		return m_info.getCompression();
	}

	@Override
	public long length() {
		return m_service.getDataSetLength(getId());
	}

	@Override
	public RecordSet read() {
		return m_service.readDataSet(getId());
	}

	@Override
	public RecordSet queryRange(Envelope range, Option<String> filterExpr) {
		return m_service.queryRange(getId(), range, filterExpr);
	}

	@Override
	public long append(RecordSet rset) {
		Objects.requireNonNull(rset, "RecordSet is null");
		
		long count = m_service.appendRecordSet(getId(), rset, Option.none());
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public long append(RecordSet rset, Plan plan) {
		Objects.requireNonNull(rset, "RecordSet is null");
		Objects.requireNonNull(plan, "Plan is null");
		
		long count = m_service.appendRecordSet(getId(), rset, Option.some(plan));
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public SpatialIndexInfo cluster(ClusterDataSetOptions opts) {
		return m_service.clusterDataSet(getId(), opts);
	}

	@Override
	public List<SpatialClusterInfo> querySpatialClusterInfo(Envelope bounds) {
		return m_service.querySpatialClusterInfo(getId(), bounds);
	}

	@Override
	public RecordSet readSpatialCluster(String quadKey, Option<String> filterExpr) {
		return m_service.readSpatialCluster(getId(), quadKey, filterExpr);
	}

	@Override
	public RecordSet sampleSpatialCluster(String quadKey, Envelope bounds, double sampleRatio) {
		return m_service.sampleSpatialCluster(getId(), quadKey, bounds, sampleRatio);
	}
	
	@Override
	public String toString() {
		return getId();
	}
}
