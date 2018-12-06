package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Map;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.vavr.control.Option;
import marmot.ExecutePlanOption;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ExecutePlanLocallyRequest;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.ExecuteProcessRequest;
import marmot.proto.service.GetOutputRecordSchemaRequest;
import marmot.proto.service.GetStreamRequest;
import marmot.proto.service.OptionalRecordResponse;
import marmot.proto.service.PlanExecutionServiceGrpc;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceBlockingStub;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceStub;
import marmot.proto.service.RecordSchemaResponse;
import marmot.protobuf.PBUtils;
import marmot.rset.PBInputStreamRecordSet;
import marmot.rset.PBRecordSetInputStream;
import marmot.support.DefaultRecord;
import utils.Throwables;

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
											Option<RecordSchema> inputSchema) {
		GetOutputRecordSchemaRequest.Builder builder
								= GetOutputRecordSchemaRequest.newBuilder()
															.setPlan(plan.toProto());
		inputSchema.map(RecordSchema::toProto)
					.forEach(builder::setInputSchema);
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
	
	public void execute(Plan plan, ExecutePlanOption... opts) {
		ExecutePlanRequest req = toExecutePlanRequest(plan, opts);
		PBUtils.handle(m_blockingStub.execute(req));
	}

	public RecordSet executeLocally(Plan plan) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_stub.executeLocally(downloader);

		// start download by sending 'stream-download' request
		ExecutePlanRequest req = toExecutePlanRequest(plan);
		InputStream is = downloader.start(req.toByteString(), channel);
		
		return PBInputStreamRecordSet.from(is);
	}

	public RecordSet executeLocally(Plan plan, RecordSet input) {
		try {
			InputStream is = PBRecordSetInputStream.from(input);
			StreamUpnDownloadClient client = new StreamUpnDownloadClient(is) {
				@Override
				protected ByteString getHeader() throws Exception {
					ExecutePlanLocallyRequest req = ExecutePlanLocallyRequest.newBuilder()
																			.setPlan(plan.toProto())
																			.setHasInputRset(true)
																			.build();
					return req.toByteString();
				}
			};
			
			InputStream stream = client.upAndDownload(m_stub.executeLocallyWithInput(client));
			return PBInputStreamRecordSet.from(stream);
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}

	public Option<Record> executeToRecord(Plan plan, ExecutePlanOption... opts) {
		RecordSchema outSchema = getOutputRecordSchema(plan, Option.none());

		ExecutePlanRequest req = toExecutePlanRequest(plan, opts);
		
		OptionalRecordResponse resp = m_blockingStub.executeToSingle(req);
		switch ( resp.getEitherCase() ) {
			case RECORD:
				return Option.some(DefaultRecord.fromProto(outSchema, resp.getRecord()));
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			case NONE:
				return Option.none();
			default:
				throw new AssertionError();
		}
	}
	
	public RecordSet executeToRecordSet(Plan plan) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
		StreamObserver<DownChunkResponse> channel = m_stub.executeToRecordSet(downloader);

		// start download by sending 'stream-download' request
		ExecutePlanRequest req = toExecutePlanRequest(plan);
		InputStream is = downloader.start(req.toByteString(), channel);
		
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
	
	static ExecutePlanRequest toExecutePlanRequest(Plan plan, ExecutePlanOption... opts) {
		ExecutePlanRequest.Builder builder = ExecutePlanRequest.newBuilder()
																.setPlan(plan.toProto());
		if ( opts.length > 0 ) {
			builder.setOptions(ExecutePlanOption.toProto(opts));
		}
		
		return builder.build();
	}
}
