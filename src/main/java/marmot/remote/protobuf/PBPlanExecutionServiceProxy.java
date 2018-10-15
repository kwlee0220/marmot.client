package marmot.remote.protobuf;

import java.util.Map;

import io.grpc.ManagedChannel;
import io.vavr.control.Option;
import marmot.ExecutePlanOption;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.ExecuteProcessRequest;
import marmot.proto.service.GetOutputRecordSchemaRequest;
import marmot.proto.service.GetStreamRequest;
import marmot.proto.service.OptionalRecordResponse;
import marmot.proto.service.PlanExecutionServiceGrpc;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceBlockingStub;
import marmot.proto.service.RecordSchemaResponse;
import marmot.proto.service.RecordSetRefResponse;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBPlanExecutionServiceProxy {
	private final PBMarmotClient m_marmot;
	private final PlanExecutionServiceBlockingStub m_stub;

	PBPlanExecutionServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
		m_marmot = marmot;
		m_stub = PlanExecutionServiceGrpc.newBlockingStub(channel);
	}
	
	public boolean getMapOutputCompression() {
		return PBUtils.handle(m_stub.getMapOutputCompression(PBUtils.VOID));
	}

	public boolean setMapOutputCompression(boolean flag) {
		return PBUtils.handle(m_stub.setMapOutputCompression(PBUtils.toProto(flag)));
	}

	public RecordSchema getOutputRecordSchema(Plan plan,
											Option<RecordSchema> inputSchema) {
		GetOutputRecordSchemaRequest.Builder builder
								= GetOutputRecordSchemaRequest.newBuilder()
															.setPlan(plan.toProto());
		inputSchema.map(RecordSchema::toProto)
					.forEach(builder::setInputSchema);
		GetOutputRecordSchemaRequest req = builder.build();
		
		RecordSchemaResponse resp = m_stub.getOutputRecordSchema(req);
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
		PBUtils.handle(m_stub.execute(req));
	}

	public RecordSet executeLocally(Plan plan, Option<RecordSet> input) {
		ExecutePlanRequest.Builder builder = ExecutePlanRequest.newBuilder()
															.setPlan(plan.toProto());
		
		if ( input.isDefined() ) {
			RecordSet rset = input.get();
			String rsetId = m_marmot.allocateRecordSet(rset.getRecordSchema());
			UploadRecordSet.start(m_marmot, rsetId, rset);
			
			builder.setInputRsetId(rsetId);
		}
		ExecutePlanRequest req = builder.build();
		
		RecordSetRefResponse resp = m_stub.executeLocally(req);
		return m_marmot.deserialize(resp);
	}

	public Option<Record> executeToRecord(Plan plan, ExecutePlanOption... opts) {
		RecordSchema outSchema = getOutputRecordSchema(plan, Option.none());

		ExecutePlanRequest req = toExecutePlanRequest(plan, opts);
		
		OptionalRecordResponse resp = m_stub.executeToSingle(req);
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
		ExecutePlanRequest req = ExecutePlanRequest.newBuilder()
													.setPlan(plan.toProto())
													.build();
		RecordSetRefResponse resp = m_stub.executeToRecordSet(req);
		return m_marmot.deserialize(resp);
	}
	
    public RecordSet executeToStream(String id, Plan plan) {
    	GetStreamRequest req = GetStreamRequest.newBuilder()
    											.setId(id)
    											.setPlan(plan.toProto())
    											.build();
    	return m_marmot.deserialize(m_stub.executeToStream(req));
    }

	public RecordSchema getProcessRecordSchema(String processId,
												Map<String, String> params) {
		ExecuteProcessRequest req = ExecuteProcessRequest.newBuilder()
														.setProcessId(processId)
														.setParams(PBUtils.toProto(params))
														.build();
		RecordSchemaResponse resp = m_stub.getProcessRecordSchema(req);
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
		PBUtils.handle(m_stub.executeProcess(req));
	}

	public void executeModule(String id) {
		PBUtils.handle(m_stub.executeModule(PBUtils.toStringProto(id)));
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
