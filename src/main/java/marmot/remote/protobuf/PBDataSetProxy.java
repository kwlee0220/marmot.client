package marmot.remote.protobuf;

import java.io.IOException;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.GeometryColumnNotExistsException;
import marmot.InsufficientThumbnailException;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.ThumbnailNotFoundException;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.IndexNotFoundException;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.geo.query.RangeQueryEstimate;
import utils.Utilities;
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
					.getOrThrow(GeometryColumnNotExistsException::new);
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
	public FOption<String> getCompressionCodecName() {
		return m_info.getCompressionCodecName();
	}

	@Override
	public DataSet updateGeometryColumnInfo(FOption<GeometryColumnInfo> gcInfo) {
		return m_service.updateGeometryColumnInfo(getId(), gcInfo);
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
	public RangeQueryEstimate estimateRangeQuery(Envelope range) throws IOException {
		return m_service.estimateRangeQuery(getId(), range);
	}

	@Override
	public RecordSet queryRange(Envelope range, int nsamples) throws IOException {
		return m_service.queryRange(getId(), range, nsamples);
	}

	@Override
	public long append(RecordSet rset) {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.empty());
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public long append(RecordSet rset, Plan plan) {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		Utilities.checkNotNullArgument(plan, "Plan is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.of(plan));
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

//	@Override
//	public void appendPlanResult(Plan plan, ExecutePlanOptions execOpts) {
//		Utilities.checkNotNullArgument(plan, "Plan is null");
//		Utilities.checkNotNullArgument(execOpts, "ExecutePlanOptions is null");
//		
//		m_info = m_service.appendPlanResult(getId(), plan, execOpts).m_info;
//	}

	@Override
	public SpatialIndexInfo cluster(ClusterDataSetOptions opts) {
		return m_service.clusterDataSet(getId(), opts);
	}

	@Override
	public void deleteSpatialCluster() {
		m_service.deleteSpatialCluster(getId());
	}

	@Override
	public RecordSet readSpatialCluster(String quadKey) {
		return m_service.readSpatialCluster(getId(), quadKey);
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
	public void createThumbnail(int sampleCount) throws IndexNotFoundException {
		m_service.createThumbnail(getId(), sampleCount);
	}

	@Override
	public boolean deleteThumbnail() {
		return m_service.deleteThumbnail(getId());
	}

	@Override
	public float getThumbnailRatio() throws ThumbnailNotFoundException {
		return m_service.getThumbnailRatio(getId());
	}
	
	@Override
	public String toString() {
		return getId();
	}
}
