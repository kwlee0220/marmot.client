package marmot.remote.protobuf;

import static marmot.ExecutePlanOptions.DEFAULT;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution.State;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.ExecuteProcessRequest;
import marmot.proto.service.ExecutionInfoProto;
import marmot.proto.service.ExecutionInfoResponse;
import marmot.proto.service.ExecutionStateResponse;
import marmot.proto.service.ExecutionStateResponse.ExecutionStateInfoProto;
import marmot.proto.service.GetOutputRecordSchemaRequest;
import marmot.proto.service.GetStreamRequest;
import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisResponse;
import marmot.proto.service.MarmotAnalysisTraverseRequest;
import marmot.proto.service.OptionalRecordResponse;
import marmot.proto.service.PlanExecutionServiceGrpc;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceBlockingStub;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceStub;
import marmot.proto.service.RecordSchemaResponse;
import marmot.proto.service.SetExecutionInfoRequest;
import marmot.proto.service.TimeoutProto;
import marmot.proto.service.WaitForFinishedRequest;
import marmot.protobuf.PBUtils;
import marmot.rset.PBInputStreamRecordSet;
import marmot.rset.PBRecordSetInputStream;
import marmot.support.DefaultRecord;
import utils.Throwables;
import utils.func.FOption;
import utils.io.Lz4Compressions;

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
	
	public void addMarmotAnalysis(MarmotAnalysis analytics) {
		MarmotAnalysisProto req = analytics.toProto();
		PBUtils.handle(m_blockingStub.addMarmotAnalysis(req));
	}
	
	public void deleteMarmotAnalysis(String id) {
		PBUtils.handle(m_blockingStub.deleteMarmotAnalysis(PBUtils.toStringProto(id)));
	}
	
	public void deleteMarmotAnalysisAll(String folder) {
		PBUtils.handle(m_blockingStub.deleteMarmotAnalysisAll(PBUtils.toStringProto(folder)));
	}
	
	public MarmotAnalysis getMarmotAnalysis(String id) {
		return toMarmotAnalysis(m_blockingStub.getMarmotAnalysis(PBUtils.toStringProto(id)));
	}
	
	public List<MarmotAnalysis> getMarmotAnalysisAllInDir(String folder, boolean recursive) {
		MarmotAnalysisTraverseRequest req = MarmotAnalysisTraverseRequest.newBuilder()
																	.setDirectory(folder)
																	.setRecursive(recursive)
																	.build();

		return PBUtils.toFStream(m_blockingStub.getMarmotAnalysisAllInDir(req))
						.map(this::toMarmotAnalysis)
						.cast(MarmotAnalysis.class)
						.toList();
	}
	
	public PBMarmotExecutionProxy startMarmotAnalysis(String id) {
		String execId = PBUtils.handle(m_blockingStub.startMarmotAnalysis(PBUtils.toStringProto(id)));
		return new PBMarmotExecutionProxy(this, execId);
	}
	
	public void executeMarmotAnalysis(String id) {
		PBUtils.handle(m_blockingStub.executeMarmotAnalysis(PBUtils.toStringProto(id)));
	}
	
	public PBMarmotExecutionProxy getMarmotExecution(String id) {
		PBMarmotExecutionProxy proxy = new PBMarmotExecutionProxy(this, id);
		proxy.getState();
		
		return proxy;
	}
	
	public PBMarmotExecutionProxy start(Plan plan, ExecutePlanOptions opts) {
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.setOptions(opts.toProto())
													.build();
		String execId = PBUtils.handle(m_blockingStub.start(req));
		return new PBMarmotExecutionProxy(this, execId);
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

	public void executeModule(String id) {
		PBUtils.handle(m_blockingStub.executeModule(PBUtils.toStringProto(id)));
	}
	
	public Tuple2<State, Throwable> getExecutionState(String id) {
		ExecutionStateResponse resp = m_blockingStub.getExecutionState(PBUtils.toStringProto(id));
		return fromExecutionStateResponse(resp);
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
	
	public Tuple2<State, Throwable> setExecutionInfo(String id, ExecutionInfoProto proto) {
		SetExecutionInfoRequest req = SetExecutionInfoRequest.newBuilder()
															.setExecId(id)
															.setExecInfo(proto)
															.build();
		ExecutionStateResponse resp = m_blockingStub.setExecutionInfo(req);
		return fromExecutionStateResponse(resp);
	}
	
	public boolean cancelExecution(String id) {
		return PBUtils.handle(m_blockingStub.cancelExecution(PBUtils.toStringProto(id)));
	}
	
	public Tuple2<State, Throwable> waitForFinished(String id) {
		WaitForFinishedRequest req = WaitForFinishedRequest.newBuilder()
															.setExecId(id)
															.build();
		ExecutionStateResponse resp = m_blockingStub.waitForFinished(req);
		return fromExecutionStateResponse(resp);
	}
	
	public Tuple2<State, Throwable> waitForFinished(String id, long timeout, TimeUnit unit) {
		TimeoutProto toProto = TimeoutProto.newBuilder()
											.setTimeout(timeout)
											.setTimeUnit(unit.name())
											.build();
		WaitForFinishedRequest req = WaitForFinishedRequest.newBuilder()
															.setExecId(id)
															.setTimeout(toProto)
															.build();
		ExecutionStateResponse resp = m_blockingStub.waitForFinished(req);
		return fromExecutionStateResponse(resp);
	}
	
	private Tuple2<State, Throwable> fromExecutionStateResponse(ExecutionStateResponse resp) {
		switch ( resp.getEitherCase() ) {
			case EXEC_STATE_INFO:
				ExecutionStateInfoProto infoProto = resp.getExecStateInfo();
				State state = PBUtils.fromExecutionStateProto(infoProto.getState());
				switch ( infoProto.getOptionalFailureCauseCase() ) {
					case FAILURE_CAUSE:
						Throwable cause = PBUtils.toException(infoProto.getFailureCause());
						return Tuple.of(state, cause);
					case OPTIONALFAILURECAUSE_NOT_SET:
						return Tuple.of(state, null);
					default:
						throw new AssertionError();
				}
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
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
			default:
				throw new AssertionError();
		}
	}
}
