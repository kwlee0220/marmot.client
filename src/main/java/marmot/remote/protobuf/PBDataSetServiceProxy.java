package marmot.remote.protobuf;


import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Envelope;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.BindDataSetOptions;
import marmot.CreateDataSetOptions;
import marmot.DataSet;
import marmot.DataSetExistsException;
import marmot.DataSetNotFoundException;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.StoreDataSetOptions;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.geo.query.RangeQueryEstimate;
import marmot.proto.LongProto;
import marmot.proto.StringProto;
import marmot.proto.service.AppendRecordSetRequest;
import marmot.proto.service.BindDataSetRequest;
import marmot.proto.service.BoolResponse;
import marmot.proto.service.ClusterDataSetRequest;
import marmot.proto.service.CreateDataSetRequest;
import marmot.proto.service.CreateKafkaTopicRequest;
import marmot.proto.service.CreateThumbnailRequest;
import marmot.proto.service.DataSetInfoResponse;
import marmot.proto.service.DataSetServiceGrpc;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceBlockingStub;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceStub;
import marmot.proto.service.DataSetTypeProto;
import marmot.proto.service.DirectoryTraverseRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.EstimateRangeQueryRequest;
import marmot.proto.service.FloatResponse;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MoveDataSetRequest;
import marmot.proto.service.MoveDirRequest;
import marmot.proto.service.QueryRangeRequest;
import marmot.proto.service.RangeQueryEstimateResponse;
import marmot.proto.service.ReadDataSetRequest;
import marmot.proto.service.ReadRawSpatialClusterRequest;
import marmot.proto.service.ReadThumbnailRequest;
import marmot.proto.service.SpatialIndexInfoResponse;
import marmot.proto.service.StringResponse;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpdateGeometryColumnInfoRequest;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBInputStreamRecordSet;
import marmot.protobuf.PBRecordSetInputStream;
import marmot.protobuf.PBUtils;
import utils.Throwables;
import utils.func.FOption;
import utils.io.Lz4Compressions;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetServiceProxy {
	private final PBMarmotClient m_marmot;
	private final DataSetServiceBlockingStub m_dsBlockingStub;
	private final DataSetServiceStub m_dsStub;

	PBDataSetServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
		m_marmot = marmot;
		m_dsStub = DataSetServiceGrpc.newStub(channel);
		m_dsBlockingStub = DataSetServiceGrpc.newBlockingStub(channel);
	}

	public PBMarmotClient getMarmotRuntime() {
		return m_marmot;
	}
	
	public DataSet createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
			throws DataSetExistsException {
		CreateDataSetRequest proto  = CreateDataSetRequest.newBuilder()
															.setId(dsId)
															.setRecordSchema(schema.toProto())
															.setOptions(opts.toProto())
															.build();
		return toDataSet(m_dsBlockingStub.createDataSet(proto));
	}
	
	public DataSet createDataSet(String dsId, Plan plan, StoreDataSetOptions opts)
		throws DataSetExistsException {
		CreateDataSetRequest proto = CreateDataSetRequest.newBuilder()
														.setId(dsId)
														.setOptions(opts.toCreateOptions().toProto())
														.build();
		return toDataSet(m_dsBlockingStub.createDataSet(proto));
	}

	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										BindDataSetOptions opts) {
		DataSetTypeProto dsTypeProto = DataSetTypeProto.valueOf(type.id());
		BindDataSetRequest.Builder builder = BindDataSetRequest.newBuilder()
													.setDataset(dsId)
													.setFilePath(srcPath)
													.setType(dsTypeProto)
													.setOptions(opts.toProto());
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
		return FStream.from(m_dsBlockingStub.getDataSetInfoAll(PBUtils.VOID))
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
	
	public DataSet updateGeometryColumnInfo(String dsId, FOption<GeometryColumnInfo> info) {
		UpdateGeometryColumnInfoRequest.Builder builder
												= UpdateGeometryColumnInfoRequest.newBuilder()
																				.setId(dsId);
		info.map(GeometryColumnInfo::toProto)
			.ifPresent(builder::setGcInfo);
		UpdateGeometryColumnInfoRequest req = builder.build();
		
		DataSetInfoResponse resp = m_dsBlockingStub.updateGeometryColumnInfo(req);
		return toDataSet(resp);		
	}
	
	public SpatialIndexInfo getDefaultSpatialIndexInfoOrNull(String dsId) {
		StringProto req = PBUtils.toStringProto(dsId);
		return handle(m_dsBlockingStub.getDefaultSpatialIndexInfo(req));
	}
	
	public RecordSet readDataSet(String dsId) throws DataSetNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<DownChunkResponse> channel = m_dsStub.readDataSet(downloader);
		
		ReadDataSetRequest req = ReadDataSetRequest.newBuilder()
												.setId(dsId)
												.setUseCompression(m_marmot.useCompression())
												.build();
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}
		return PBInputStreamRecordSet.from(is);
	}
	
	public RangeQueryEstimate estimateRangeQuery(String dsId, Envelope range) {
		EstimateRangeQueryRequest req = EstimateRangeQueryRequest.newBuilder()
																.setDatasetId(dsId)
																.setRange(PBUtils.toProto(range))
																.build();
		RangeQueryEstimateResponse resp = m_dsBlockingStub.estimateRangeQuery(req);
		switch ( resp.getEitherCase() ) {
			case ESTIMATE:
				return RangeQueryEstimate.fromProto(resp.getEstimate());
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public RecordSet queryRange(String dsId, Envelope range, int nsamples)
		throws DataSetNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_dsStub.queryRange(downloader);
		
		QueryRangeRequest req = QueryRangeRequest.newBuilder()
												.setId(dsId)
												.setRange(PBUtils.toProto(range))
												.setSampleCount(nsamples)
												.setUseCompression(m_marmot.useCompression())
												.build();

		// start download by sending 'stream-download' request
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}
		
		return PBInputStreamRecordSet.from(is);
	}
	
	public long appendRecordSet(String dsId, RecordSet rset, FOption<Plan> plan) {
		try {
			InputStream is = PBRecordSetInputStream.from(rset);
			if ( m_marmot.useCompression() ) {
				is = Lz4Compressions.compress(is);
			}
			
			StreamUploadSender uploader = new StreamUploadSender(is) {
				@Override
				protected ByteString getHeader() throws Exception {
					AppendRecordSetRequest.Builder builder
									= AppendRecordSetRequest.newBuilder()
															.setId(dsId)
															.setUseCompression(m_marmot.useCompression());
					plan.map(Plan::toProto).ifPresent(builder::setPlan);
					AppendRecordSetRequest req = builder.build();
					return req.toByteString();
				}
			};
			StreamObserver<UpChunkRequest> channel = m_dsStub.appendRecordSet(uploader);
			uploader.setChannel(channel);
			uploader.start();
			
			ByteString ret = uploader.get();
			return LongProto.parseFrom(ret).getValue();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	
//	public PBDataSetProxy appendPlanResult(String dsId, Plan plan, ExecutePlanOptions execOpts)
//		throws DataSetNotFoundException {
//		ExecutePlanRequest execPlan = ExecutePlanRequest.newBuilder()
//														.setPlan(plan.toProto())
//														.setOptions(execOpts.toProto())
//														.build();
//		AppendPlanResultRequest req = AppendPlanResultRequest.newBuilder()
//															.setId(dsId)
//															.setPlanExec(execPlan)
//															.build();
//		DataSetInfoResponse resp = m_dsBlockingStub.appendPlanResult(req);
//		return toDataSet(resp);
//	}
	
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
		ClusterDataSetRequest req = ClusterDataSetRequest.newBuilder()
														.setId(id)
														.setOptions(opts.toProto())
														.build();
		return handle(m_dsBlockingStub.clusterDataSet(req));
	}

	public void deleteSpatialCluster(String id) {
		PBUtils.handle(m_dsBlockingStub.deleteSpatialCluster(PBUtils.toStringProto(id)));
	}
	
	public RecordSet readSpatialCluster(String dsId, String quadKey) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		ReadRawSpatialClusterRequest.Builder builder = ReadRawSpatialClusterRequest.newBuilder()
																.setDatasetId(dsId)
																.setQuadKey(quadKey)
																.setUseCompression(m_marmot.useCompression());
		ReadRawSpatialClusterRequest req = builder.build();
		StreamObserver<DownChunkResponse> channel = m_dsStub.readRawSpatialCluster(downloader);
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}
		return PBInputStreamRecordSet.from(is);
	}

	public List<String> getDirAll() {
		return FStream.from(m_dsBlockingStub.getDirAll(PBUtils.VOID))
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
		PBUtils.handle(m_dsBlockingStub.createKafkaTopic(req));
	}
	
	public boolean hasThumbnail(String dsId) {
		BoolResponse resp = m_dsBlockingStub.hasThumbnail(PBUtils.toStringProto(dsId));
		return PBUtils.handle(resp);
	}
	
	public void createThumbnail(String dsId, int sampleCount) {
		CreateThumbnailRequest req = CreateThumbnailRequest.newBuilder()
															.setId(dsId)
															.setCount(sampleCount)
															.build();
		PBUtils.handle(m_dsBlockingStub.createThumbnail(req));
	}
	
	public boolean deleteThumbnail(String dsId) {
		BoolResponse resp = m_dsBlockingStub.deleteThumbnail(PBUtils.toStringProto(dsId));
		return PBUtils.handle(resp);
	}
	
	public RecordSet readThumbnail(String dsId, Envelope range, int count) {
		ReadThumbnailRequest req = ReadThumbnailRequest.newBuilder()
														.setId(dsId)
														.setRange(PBUtils.toProto(range))
														.setCount(count)
														.setUseCompression(m_marmot.useCompression())
														.build();

		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_dsStub.readThumbnail(downloader);
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}

		return PBInputStreamRecordSet.from(is);
	}
	
	public float getThumbnailRatio(String dsId) {
		FloatResponse resp = m_dsBlockingStub.getThumbnailRatio(PBUtils.toStringProto(dsId));
		return PBUtils.handle(resp);
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
