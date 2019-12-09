package marmot.remote.protobuf;

import static marmot.ExecutePlanOptions.DEFAULT;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.exec.AnalysisNotFoundException;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.proto.StringProto;
import marmot.proto.service.AddAnalysisRequest;
import marmot.proto.service.DeleteAnalysisRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.ExecuteProcessRequest;
import marmot.proto.service.ExecutionInfoListResponse;
import marmot.proto.service.ExecutionInfoProto;
import marmot.proto.service.ExecutionInfoResponse;
import marmot.proto.service.GetOutputRecordSchemaRequest;
import marmot.proto.service.GetStreamRequest;
import marmot.proto.service.MarmotAnalysisResponse;
import marmot.proto.service.OptionalRecordResponse;
import marmot.proto.service.PlanExecutionServiceGrpc;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceBlockingStub;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceStub;
import marmot.proto.service.RecordSchemaResponse;
import marmot.proto.service.SetExecutionInfoRequest;
import marmot.proto.service.TimeoutProto;
import marmot.proto.service.WaitForFinishedRequest;
import marmot.protobuf.PBInputStreamRecordSet;
import marmot.protobuf.PBRecordSetInputStream;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import utils.Throwables;
import utils.func.FOption;
import utils.io.Lz4Compressions;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBPlanExecutionServiceProxy {
	private final PBMarmotClient m_marmot;
	private final PlanExecutionServiceBlockingStub m_blockingStub;
	private final PlanExecutionServiceStub m_stub;

	PBPlanExecutionServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
		m_marmot = marmot;
		m_blockingStub = PlanExecutionServiceGrpc.newBlockingStub(channel);
		m_stub = PlanExecutionServiceGrpc.newStub(channel);
	}
	
	PBMarmotClient getMarmotRuntime() {
		return m_marmot;
	}

	public RecordSchema getOutputRecordSchema(Plan plan,
											FOption<RecordSchema> inputSchema) {
		GetOutputRecordSchemaRequest.Builder builder
								= GetOutputRecordSchemaRequest.newBuilder()
															.setPlan(plan.toProto());
		inputSchema.map(RecordSchema::toProto)
					.ifPresent(builder::setInputSchema);
		GetOutputRecordSchemaRequest req = builder.build();
		
		RecordSchemaResponse resp = m_blockingStub.getOutputRecordSchema(req);
		switch ( resp.getEitherCase() ) {
			case RECORD_SCHEMA:
				return RecordSchema.fromProto(resp.getRecordSchema());
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}

	public MarmotAnalysis getAnalysis(String id) {
		MarmotAnalysis analy = findAnalysis(id);
		if ( analy == null ) {
			throw new AnalysisNotFoundException(id);
		}
		
		return analy;
	}
	
	public MarmotAnalysis findAnalysis(String id) {
		return toMarmotAnalysis(m_blockingStub.findAnalysis(PBUtils.toStringProto(id)));
	}

	public CompositeAnalysis findParentAnalysis(String id) {
		StringProto req = PBUtils.toStringProto(id);
		return (CompositeAnalysis)toMarmotAnalysis(m_blockingStub.findParentAnalysis(req));
	}

	public List<CompositeAnalysis> getAncestorAnalysisAll(String id) {
		StringProto req = PBUtils.toStringProto(id);
		return PBUtils.toFStream(m_blockingStub.getAncestorAnalysisAll(req))
						.map(this::toMarmotAnalysis)
						.cast(CompositeAnalysis.class)
						.toList();
	}

	public List<MarmotAnalysis> getDescendantAnalysisAll(String id) {
		StringProto req = PBUtils.toStringProto(id);
		return PBUtils.toFStream(m_blockingStub.getDescendantAnalysisAll(req))
						.map(this::toMarmotAnalysis)
						.cast(MarmotAnalysis.class)
						.toList();
	}

	public List<MarmotAnalysis> getAnalysisAll() {
		return PBUtils.toFStream(m_blockingStub.getAnalysisAll(PBUtils.VOID))
						.map(this::toMarmotAnalysis)
						.cast(MarmotAnalysis.class)
						.toList();
	}
	
	public void addAnalysis(MarmotAnalysis analysis, boolean force) {
		AddAnalysisRequest req = AddAnalysisRequest.newBuilder()
													.setAnalysis(analysis.toProto())
													.setForce(force)
													.build();
		PBUtils.handle(m_blockingStub.addAnalysis(req));
	}

	public void deleteAnalysis(String id, boolean recursive) {
		DeleteAnalysisRequest req = DeleteAnalysisRequest.newBuilder()
															.setId(id)
															.setRecursive(recursive)
															.build();
		PBUtils.handle(m_blockingStub.deleteAnalysis(req));
	}

	public void deleteAnalysisAll() {
		PBUtils.handle(m_blockingStub.deleteAnalysisAll(PBUtils.VOID));
	}
	
	public PBMarmotExecutionProxy startAnalysis(MarmotAnalysis analysis) {
		return (PBMarmotExecutionProxy)fromInfoResponse(m_blockingStub.startAnalysis(analysis.toProto()));
	}
	
	public void executeAnalysis(MarmotAnalysis analysis) {
		PBUtils.handle(m_blockingStub.executeAnalysis(analysis.toProto()));
	}
	
	public PBMarmotExecutionProxy getMarmotExecution(String id) {
		return fromInfoResponse(m_blockingStub.getExecutionInfo(PBUtils.toStringProto(id)));
	}
	
	public List<MarmotExecution> getMarmotExecutionAll() {
		ExecutionInfoListResponse resp = m_blockingStub.getExecutionInfoList(PBUtils.VOID);
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO_LIST:
				return FStream.from(resp.getExecInfoList().getExecInfoList())
								.map(this::fromProto)
								.cast(MarmotExecution.class)
								.toList();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	private PBMarmotExecutionProxy fromInfoResponse(ExecutionInfoResponse resp) {
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO:
				return fromProto(resp.getExecInfo());
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	private PBMarmotExecutionProxy fromProto(ExecutionInfoProto info) {
		return new PBMarmotExecutionProxy(this, info);
	}
	
	public PBMarmotExecutionProxy start(Plan plan, ExecutePlanOptions opts) {
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(opts.toProto())
													.build();
		return fromInfoResponse(m_blockingStub.start(req));
	}
	
	public void execute(Plan plan, ExecutePlanOptions opts) {
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(opts.toProto())
													.build();
		PBUtils.handle(m_blockingStub.execute(req));
	}

	public RecordSet executeLocally(Plan plan) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_stub.executeLocally(downloader);

		// start download by sending 'stream-download' request
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(DEFAULT.toProto())
													.setUseCompression(m_marmot.useCompression())
													.build();
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}
		
		return PBInputStreamRecordSet.from(is);
	}

	public RecordSet executeLocally(Plan plan, RecordSet input) {
		try {
			InputStream is = PBRecordSetInputStream.from(input);
			StreamUpnDownloadClient client = new StreamUpnDownloadClient(is) {
				@Override
				protected ByteString getHeader() throws Exception {
					ExecutePlanRequest req
								= ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setHasInputRset(true)
													.setUseCompression(m_marmot.useCompression())
													.build();
					return req.toByteString();
				}
			};
			
			InputStream stream = client.upAndDownload(m_stub.executeLocallyWithInput(client));
			if ( m_marmot.useCompression() ) {
				stream = Lz4Compressions.decompress(stream);
			}
			
			return PBInputStreamRecordSet.from(stream);
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}

	public FOption<Record> executeToRecord(Plan plan, ExecutePlanOptions opts) {
		RecordSchema outSchema = getOutputRecordSchema(plan, FOption.empty());

		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(opts.toProto())
													.build();
		OptionalRecordResponse resp = m_blockingStub.executeToSingle(req);
		switch ( resp.getEitherCase() ) {
			case RECORD:
				return FOption.of(DefaultRecord.fromProto(outSchema, resp.getRecord()));
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			case NONE:
				return FOption.empty();
			default:
				throw new AssertionError();
		}
	}
	
	public RecordSet executeToRecordSet(Plan plan) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_stub.executeToRecordSet(downloader);

		// start download by sending 'stream-download' request
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(DEFAULT.toProto())
													.setUseCompression(m_marmot.useCompression())
													.build();
		InputStream is = downloader.start(req.toByteString(), channel);
		if ( m_marmot.useCompression() ) {
			is = Lz4Compressions.decompress(is);
		}
		
		return PBInputStreamRecordSet.from(is);
	}
	
    public RecordSet executeToStream(String id, Plan plan) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_stub.executeToStream(downloader);

		// start download by sending 'stream-download' request
		GetStreamRequest req = GetStreamRequest.newBuilder()
												.setId(id)
												.setPlan(plan.toProto())
												.build();
		InputStream is = downloader.start(req.toByteString(), channel);
		
		return PBInputStreamRecordSet.from(is);
    }

	public RecordSchema getProcessRecordSchema(String processId,
												Map<String, String> params) {
		ExecuteProcessRequest req = ExecuteProcessRequest.newBuilder()
														.setProcessId(processId)
														.setParams(PBUtils.toProto(params))
														.build();
		RecordSchemaResponse resp = m_blockingStub.getProcessRecordSchema(req);
		switch ( resp.getEitherCase() ) {
			case RECORD_SCHEMA:
				return RecordSchema.fromProto(resp.getRecordSchema());
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}

	public void executeProcess(String processId, Map<String, String> params) {
		ExecuteProcessRequest req = ExecuteProcessRequest.newBuilder()
														.setProcessId(processId)
														.setParams(PBUtils.toProto(params))
														.build();
		PBUtils.handle(m_blockingStub.executeProcess(req));
	}
	
	public ExecutionInfoProto getExecutionInfo(String id) {
		ExecutionInfoResponse resp = m_blockingStub.getExecutionInfo(PBUtils.toStringProto(id));
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO:
				return resp.getExecInfo();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public ExecutionInfoProto setExecutionInfo(String id, ExecutionInfoProto proto) {
		SetExecutionInfoRequest req = SetExecutionInfoRequest.newBuilder()
															.setExecId(id)
															.setExecInfo(proto)
															.build();
		ExecutionInfoResponse resp = m_blockingStub.setExecutionInfo(req);
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO:
				return resp.getExecInfo();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public boolean cancelExecution(String id) {
		return PBUtils.handle(m_blockingStub.cancelExecution(PBUtils.toStringProto(id)));
	}
	
	public ExecutionInfoProto waitForFinished(String id) {
		WaitForFinishedRequest req = WaitForFinishedRequest.newBuilder()
															.setExecId(id)
															.build();
		ExecutionInfoResponse resp = m_blockingStub.waitForFinished(req);
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO:
				return resp.getExecInfo();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public ExecutionInfoProto waitForFinished(String id, long timeout, TimeUnit unit) {
		TimeoutProto toProto = TimeoutProto.newBuilder()
											.setTimeout(timeout)
											.setTimeUnit(unit.name())
											.build();
		WaitForFinishedRequest req = WaitForFinishedRequest.newBuilder()
															.setExecId(id)
															.setTimeout(toProto)
															.build();
		ExecutionInfoResponse resp = m_blockingStub.waitForFinished(req);
		switch ( resp.getEitherCase() ) {
			case EXEC_INFO:
				return resp.getExecInfo();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
//	private Tuple2<State, Throwable> fromExecutionStateResponse(ExecutionStateResponse resp) {
//		switch ( resp.getEitherCase() ) {
//			case EXEC_STATE_INFO:
//				ExecutionStateInfoProto infoProto = resp.getExecStateInfo();
//				State state = PBUtils.fromExecutionStateProto(infoProto.getState());
//				switch ( infoProto.getOptionalFailureCauseCase() ) {
//					case FAILURE_CAUSE:
//						Throwable cause = PBUtils.toException(infoProto.getFailureCause());
//						return Tuple.of(state, cause);
//					case OPTIONALFAILURECAUSE_NOT_SET:
//						return Tuple.of(state, null);
//					default:
//						throw new AssertionError();
//				}
//			case ERROR:
//				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
//			default:
//				throw new AssertionError();
//		}
//	}
	
	public void ping() {
		m_blockingStub.ping(PBUtils.VOID);
	}
	
	private MarmotAnalysis toMarmotAnalysis(MarmotAnalysisResponse resp) {
		switch ( resp.getEitherCase() ) {
			case ANALYSIS:
				return MarmotAnalysis.fromProto(resp.getAnalysis());
			case ERROR:
				Throwables.sneakyThrow(PBUtils.toException(resp.getError()));
				throw new AssertionError();
			case EITHER_NOT_SET:
				return null;
			default:
				throw new AssertionError();
		}
	}
}
