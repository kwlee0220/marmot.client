package marmot.remote.protobuf;


import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import io.grpc.ManagedChannel;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.DataSetExistsException;
import marmot.DataSetNotFoundException;
import marmot.DataSetOption;
import marmot.DataSetType;
import marmot.ExecutePlanOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.proto.StringProto;
import marmot.proto.service.AppendRecordSetRequest;
import marmot.proto.service.BinaryChunkProto;
import marmot.proto.service.BindDataSetRequest;
import marmot.proto.service.ClusterDataSetRequest;
import marmot.proto.service.CreateDataSetRequest;
import marmot.proto.service.CreateKafkaTopicRequest;
import marmot.proto.service.DataSetInfoResponse;
import marmot.proto.service.DataSetOptionsProto;
import marmot.proto.service.DataSetServiceGrpc;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceBlockingStub;
import marmot.proto.service.DataSetTypeProto;
import marmot.proto.service.DirectoryTraverseRequest;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MoveDataSetRequest;
import marmot.proto.service.MoveDirRequest;
import marmot.proto.service.QueryRangeRequest;
import marmot.proto.service.QuerySpatialClusterInfoRequest;
import marmot.proto.service.ReadSpatialClusterRequest;
import marmot.proto.service.RecordSetRefResponse;
import marmot.proto.service.SampleSpatialClusterRequest;
import marmot.proto.service.SpatialClusterInfoProto;
import marmot.proto.service.SpatialClusterInfoResponse;
import marmot.proto.service.SpatialIndexInfoResponse;
import marmot.proto.service.StringResponse;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBUtils;
import utils.Throwables;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetServiceProxy {
	private final PBMarmotClient m_marmot;
	private final DataSetServiceBlockingStub m_dsBlockingStub;
//	private final DataSetServiceStub m_dsStub;

	PBDataSetServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
		m_marmot = marmot;
//		m_dsStub = DataSetServiceGrpc.newStub(channel);
		m_dsBlockingStub = DataSetServiceGrpc.newBlockingStub(channel);
	}

	public PBMarmotClient getMarmotRuntime() {
		return m_marmot;
	}
	
	public DataSet createDataSet(String dsId, RecordSchema schema, DataSetOption... opts)
			throws DataSetExistsException {
		CreateDataSetRequest.Builder builder = CreateDataSetRequest.newBuilder()
																.setId(dsId)
																.setRecordSchema(schema.toProto());
		if ( opts.length > 0 ) {
			DataSetOptionsProto optsProto = DataSetOption.toProto(Arrays.asList(opts));
			builder.setOptions(optsProto);
		}
		CreateDataSetRequest req = builder.build();
		
		return toDataSet(m_dsBlockingStub.createDataSet(req));
	}
	
	public DataSet createDataSet(String dsId, Plan plan, ExecutePlanOption[] execOpts,
								DataSetOption... opts) throws DataSetExistsException {
		ExecutePlanRequest execPlan = PBPlanExecutionServiceProxy.toExecutePlanRequest(plan, execOpts);
		CreateDataSetRequest.Builder builder = CreateDataSetRequest.newBuilder()
														.setId(dsId)
														.setPlanExec(execPlan);
		if ( opts.length > 0 ) {
			DataSetOptionsProto optsProto = DataSetOption.toProto(Arrays.asList(opts));
			builder.setOptions(optsProto);
		}
		CreateDataSetRequest req = builder.build();
		
		return toDataSet(m_dsBlockingStub.createDataSet(req));
	}
	
	public DataSet createDataSet(String dsId, Plan plan, RecordSet input, DataSetOption... opts)
		throws DataSetExistsException {
		String rsetId = m_marmot.allocateRecordSet(input.getRecordSchema());
		UploadRecordSet.start(m_marmot, rsetId, input);

		ExecutePlanRequest execPlan = ExecutePlanRequest.newBuilder()
														.setPlan(plan.toProto())
														.setInputRsetId(rsetId)
														.build();
		CreateDataSetRequest.Builder builder = CreateDataSetRequest.newBuilder()
														.setId(dsId)
														.setPlanExec(execPlan);
		if ( opts.length > 0 ) {
			DataSetOptionsProto optsProto = DataSetOption.toProto(Arrays.asList(opts));
			builder.setOptions(optsProto);
		}
		CreateDataSetRequest req = builder.build();
		
		return toDataSet(m_dsBlockingStub.createDataSet(req));
	}

	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										Option<GeometryColumnInfo> geomColInfo) {
		DataSetTypeProto dsTypeProto = DataSetTypeProto.valueOf(type.id());
		BindDataSetRequest.Builder builder = BindDataSetRequest.newBuilder()
													.setDataset(dsId)
													.setFilePath(srcPath)
													.setType(dsTypeProto);
		geomColInfo.forEach(info -> builder.setGeometryInfo(info.toProto()));
		BindDataSetRequest req = builder.build();
		
		return toDataSet(m_dsBlockingStub.bindDataSet(req));
	}
	
	public PBDataSetProxy getDataSet(String id) {
		DataSetInfoResponse resp = m_dsBlockingStub.getDataSetInfo(PBUtils.toStringProto(id));
		return toDataSet(resp);
	}

	public DataSet getDataSetOrNull(String id) {
		DataSetInfoResponse resp = m_dsBlockingStub.getDataSetInfo(PBUtils.toStringProto(id));
		try {
			return toDataSet(resp);
		}
		catch ( DataSetNotFoundException e ) {
			return null;
		}
	}

	public List<DataSet> getDataSetAll() {
		return FStream.of(m_dsBlockingStub.getDataSetInfoAll(PBUtils.VOID))
						.map(this::toDataSet)
						.cast(DataSet.class)
						.toList();
	}

	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
													.setDirectory(folder)
													.setRecursive(recursive)
													.build();
		return PBUtils.toFStream(m_dsBlockingStub.getDataSetInfoAllInDir(req))
						.map(this::toDataSet)
						.cast(DataSet.class)
						.toList();
	}
	
	public SpatialIndexInfo getDefaultSpatialIndexInfoOrNull(String dsId) {
		StringProto req = PBUtils.toStringProto(dsId);
		return handle(m_dsBlockingStub.getDefaultSpatialIndexInfo(req));
	}
	
	public RecordSet readDataSet(String dsId) throws DataSetNotFoundException {
		RecordSetRefResponse resp = m_dsBlockingStub.readDataSet(PBUtils.toStringProto(dsId));
		return m_marmot.deserialize(resp);
	}
	
	public RecordSet queryRange(String dsId, Envelope range, Option<String> filterExpr)
		throws DataSetNotFoundException {
		QueryRangeRequest.Builder builder = QueryRangeRequest.newBuilder()
															.setId(dsId)
															.setRange(PBUtils.toProto(range));
		filterExpr.forEach(builder::setFilterExpr);
		QueryRangeRequest req = builder.build();
		
		RecordSetRefResponse resp = m_dsBlockingStub.queryRange(req);
		return m_marmot.deserialize(resp);
	}
	
	public long appendRecordSet(String dsId, RecordSet rset, Option<Plan> plan) {
		try {
			String rsetId = m_marmot.allocateRecordSet(rset.getRecordSchema());
			UploadRecordSet.start(m_marmot, rsetId, rset);
			
			AppendRecordSetRequest.Builder builder = AppendRecordSetRequest.newBuilder()
																.setId(dsId)
																.setRsetId(rsetId);
			plan.map(Plan::toProto).forEach(builder::setPlan);
			AppendRecordSetRequest req = builder.build();
			
			return PBUtils.handle(m_dsBlockingStub.appendRecordSet(req));
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	public long getDataSetLength(String id) {
		return PBUtils.handle(m_dsBlockingStub.getDataSetLength(PBUtils.toStringProto(id)));
	}
	
	public void moveDataSet(String srcId, String tarId) {
		VoidResponse resp = m_dsBlockingStub.moveDataSet(MoveDataSetRequest.newBuilder()
																	.setSrcId(srcId)
																	.setDestId(tarId)
																	.build());
		PBUtils.handle(resp);
	}

	public boolean deleteDataSet(String id) {
		return PBUtils.handle(m_dsBlockingStub.deleteDataSet(PBUtils.toStringProto(id)));
	}

	public SpatialIndexInfo clusterDataSet(String id, ClusterDataSetOptions opts) {
		ClusterDataSetRequest.Builder builder = ClusterDataSetRequest.newBuilder()
																	.setId(id);
		opts.quadKeyFilePath().forEach(builder::setQuadKeyFile);
		opts.sampleRatio().forEach(builder::setSampleRatio);
		opts.blockSize().forEach(builder::setBlockSize);
		opts.blockFillRatio().forEach(builder::setBlockFillRatio);
		ClusterDataSetRequest req = builder.build();
		
		return handle(m_dsBlockingStub.clusterDataSet(req));
	}

	public List<SpatialClusterInfo> querySpatialClusterInfo(String dsId, Envelope bounds) {
		QuerySpatialClusterInfoRequest req = QuerySpatialClusterInfoRequest.newBuilder()
													.setDatasetId(dsId)
													.setBounds(PBUtils.toProto(bounds))
													.build();
		Iterator<SpatialClusterInfoResponse> respIter = m_dsBlockingStub.querySpatialClusterInfo(req);
		
		List<SpatialClusterInfo> infoList = Lists.newArrayList();
		while ( respIter.hasNext() ) {
			SpatialClusterInfoResponse resp = respIter.next();
			switch ( resp.getEitherCase() ) {
				case SPATIAL_CLUSTER_INFO:
					SpatialClusterInfoProto infoProto = resp.getSpatialClusterInfo();
					infoList.add(SpatialClusterInfo.fromProto(infoProto));
					break;
				case ERROR:
					Exception cause = PBUtils.toException(resp.getError());
					throw Throwables.toRuntimeException(cause);
				default:
					throw new AssertionError();
			}
		}
		
		return infoList;
	}
	
	public RecordSet readSpatialCluster(String dsId, String quadKey, Option<String> filterExpr) {
		ReadSpatialClusterRequest.Builder builder = ReadSpatialClusterRequest.newBuilder()
																.setDatasetId(dsId)
																.setQuadKey(quadKey);
		filterExpr.forEach(builder::setFilterEpxr);
		ReadSpatialClusterRequest req = builder.build();
		
		RecordSetRefResponse resp = m_dsBlockingStub.readSpatialCluster(req);
		return m_marmot.deserialize(resp);
	}
	
	public InputStream readRawSpatialCluster(String dsId, String quadKey) {
		ReadSpatialClusterRequest.Builder builder = ReadSpatialClusterRequest.newBuilder()
																.setDatasetId(dsId)
																.setQuadKey(quadKey);
		ReadSpatialClusterRequest req = builder.build();
		Iterator<BinaryChunkProto> resp = m_dsBlockingStub.readRawSpatialCluster(req);
		
		return PBUtils.toInputStream(resp);
	}
	
	public RecordSet sampleSpatialCluster(String dsId, String quadKey, Envelope bounds, double sampleRatio) {
		SampleSpatialClusterRequest req = SampleSpatialClusterRequest.newBuilder()
															.setDatasetId(dsId)
															.setQuadKey(quadKey)
															.setBounds(PBUtils.toProto(bounds))
															.setSampleRatio(sampleRatio)
															.build();
		
		RecordSetRefResponse resp = m_dsBlockingStub.sampleSpatialCluster(req);
		return m_marmot.deserialize(resp);
	}

	public List<String> getDirAll() {
		return FStream.of(m_dsBlockingStub.getDirAll(PBUtils.VOID))
						.map(StringResponse::getValue)
						.toList();
	}

	public List<String> getSubDirAll(String folder, boolean recursive) {
		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
													.setDirectory(folder)
													.setRecursive(recursive)
													.build();
		Iterator<StringResponse> resp = m_dsBlockingStub.getSubDirAll(req);
		return PBUtils.toFStream(resp)
						.map(StringResponse::getValue)
						.toList();
	}

	public String getParentDir(String folder) {
		StringProto req = PBUtils.toStringProto(folder);
		StringResponse resp = m_dsBlockingStub.getParentDir(req);
		return PBUtils.handle(resp);
	}

	public void renameDir(String srcPath, String tarPath) {
		MoveDirRequest req = MoveDirRequest.newBuilder()
												.setSrcPath(srcPath)
												.setDestPath(tarPath)
												.build();
		VoidResponse resp = m_dsBlockingStub.moveDir(req);
		PBUtils.handle(resp);
	}

	public void deleteDir(String folder) {
		StringProto req = PBUtils.toStringProto(folder);
		VoidResponse resp = m_dsBlockingStub.deleteDir(req);
		PBUtils.handle(resp);
	}

	public long getBlockSize(String id) {
		LongResponse resp = m_dsBlockingStub.getBlockSize(PBUtils.toStringProto(id));
		return PBUtils.handle(resp);
	}
	
	public void createKafkaTopic(String topic, boolean force) {
		CreateKafkaTopicRequest req = CreateKafkaTopicRequest.newBuilder()
															.setTopic(topic)
															.setForce(force)
															.build();
		m_dsBlockingStub.createKafkaTopic(req);
	}
	
	PBDataSetProxy toDataSet(DataSetInfoResponse resp) {
		switch ( resp.getEitherCase() ) {
			case DATASET_INFO:
				return new PBDataSetProxy(this, DataSetInfo.fromProto(resp.getDatasetInfo()));
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	private SpatialIndexInfo handle(SpatialIndexInfoResponse resp) {
		switch ( resp.getEitherCase() ) {
			case INDEX_INFO:
				return SpatialIndexInfo.fromProto(resp.getIndexInfo());
			case NONE:
				return null;
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
}
