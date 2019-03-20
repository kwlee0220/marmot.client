package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import com.vividsolutions.jts.geom.Envelope;

import marmot.ClusterNotFoundException;
import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.GeometryColumnNotExistsException;
import marmot.InsufficientThumbnailException;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.ThumbnailNotFoundException;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.IndexNotFoundException;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;
import utils.func.FOption;

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
		return m_info.getGeometryColumnInfo().isPresent();
	}

	@Override
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_info.getGeometryColumnInfo()
					.getOrElseThrow(() -> new GeometryColumnNotExistsException());
	}

	@Override
	public Envelope getBounds() {
		if ( hasGeometryColumn() ) {
			return new Envelope(m_info.getBounds());
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
	public FOption<SpatialIndexInfo> getDefaultSpatialIndexInfo() throws IndexNotFoundException {
		SpatialIndexInfo idxInfo = m_service.getDefaultSpatialIndexInfoOrNull(getId());
		return FOption.ofNullable(idxInfo);
	}

	@Override
	public boolean isCompressed() {
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
	public RecordSet queryRange(Envelope range, FOption<String> filterExpr) {
		return m_service.queryRange(getId(), range, filterExpr);
	}

	@Override
	public long append(RecordSet rset) {
		Objects.requireNonNull(rset, "RecordSet is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.empty());
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public long append(RecordSet rset, Plan plan) {
		Objects.requireNonNull(rset, "RecordSet is null");
		Objects.requireNonNull(plan, "Plan is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.of(plan));
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public SpatialIndexInfo cluster(ClusterDataSetOptions opts) {
		return m_service.clusterDataSet(getId(), opts);
	}

	@Override
	public void deleteSpatialCluster() {
		m_service.deleteSpatialCluster(getId());
	}

	@Override
	public List<SpatialClusterInfo> querySpatialClusterInfo(Envelope bounds) {
		return m_service.querySpatialClusterInfo(getId(), bounds);
	}

	@Override
	public InputStream readRawSpatialCluster(String quadKey) {
		return m_service.readRawSpatialCluster(getId(), quadKey);
	}

	@Override
	public boolean hasThumbnail() {
		return m_service.hasThumbnail(getId());
	}

	@Override
	public RecordSet readThumbnail(Envelope bounds, int count)
		throws ThumbnailNotFoundException, InsufficientThumbnailException {
		return m_service.readThumbnail(getId(), bounds, count);
	}

	@Override
	public void createThumbnail(int sampleCount) throws ClusterNotFoundException {
		m_service.createThumbnail(getId(), sampleCount);
	}

	@Override
	public boolean deleteThumbnail() {
		return m_service.deleteThumbnail(getId());
	}
	
	@Override
	public String toString() {
		return getId();
	}
}
