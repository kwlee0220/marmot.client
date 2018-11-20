package marmot.remote.protobuf;


import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Envelope;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
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
import marmot.proto.LongProto;
import marmot.proto.StringProto;
import marmot.proto.service.AppendRecordSetRequest;
import marmot.proto.service.BindDataSetRequest;
import marmot.proto.service.ClusterDataSetRequest;
import marmot.proto.service.CreateDataSetRequest;
import marmot.proto.service.CreateKafkaTopicRequest;
import marmot.proto.service.DataSetInfoResponse;
import marmot.proto.service.DataSetOptionsProto;
import marmot.proto.service.DataSetServiceGrpc;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceBlockingStub;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceStub;
import marmot.proto.service.DataSetTypeProto;
import marmot.proto.service.DirectoryTraverseRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MoveDataSetRequest;
import marmot.proto.service.MoveDirRequest;
import marmot.proto.service.QueryRangeRequest;
import marmot.proto.service.QuerySpatialClusterInfoRequest;
import marmot.proto.service.ReadRawSpatialClusterRequest;
import marmot.proto.service.SpatialClusterInfoProto;
import marmot.proto.service.SpatialClusterInfoResponse;
import marmot.proto.service.SpatialIndexInfoResponse;
import marmot.proto.service.StringResponse;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBUtils;
import marmot.rset.PBInputStreamRecordSet;
import marmot.rset.PBRecordSetInputStream;
import utils.Throwables;
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
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<DownChunkResponse> channel = m_dsStub.readDataSet(downloader);
		InputStream is = downloader.receive(PBUtils.toStringProto(dsId).toByteString(), channel);
		
		return PBInputStreamRecordSet.from(is);
	}
	
	public RecordSet queryRange(String dsId, Envelope range, Option<String> filterExpr)
		throws DataSetNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_dsStub.readDataSet(downloader);
		
		QueryRangeRequest.Builder builder = QueryRangeRequest.newBuilder()
															.setId(dsId)
															.setRange(PBUtils.toProto(range));
		filterExpr.forEach(builder::setFilterExpr);
		QueryRangeRequest req = builder.build();

		// start download by sending 'stream-download' request
		InputStream is = downloader.receive(req.toByteString(), channel);
		
		return PBInputStreamRecordSet.from(is);
	}
	
	public long appendRecordSet(String dsId, RecordSet rset, Option<Plan> plan) {
		try {
			InputStream is = PBRecordSetInputStream.from(rset);
			StreamUploaderSender uploader = new StreamUploaderSender(is) {
				@Override
				protected ByteString getHeader() throws Exception {
					AppendRecordSetRequest.Builder builder = AppendRecordSetRequest.newBuilder()
																					.setId(dsId);
					plan.map(Plan::toProto).forEach(builder::setPlan);
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
	
	public InputStream readRawSpatialCluster(String dsId, String quadKey) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		ReadRawSpatialClusterRequest.Builder builder = ReadRawSpatialClusterRequest.newBuilder()
																.setDatasetId(dsId)
																.setQuadKey(quadKey);
		ReadRawSpatialClusterRequest req = builder.build();
		StreamObserver<DownChunkResponse> channel = m_dsStub.readDataSet(downloader);
		return downloader.receive(req.toByteString(), channel);
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
