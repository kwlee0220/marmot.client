package marmot.remote.protobuf;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import marmot.Record;
import marmot.RecordSet;
import marmot.proto.service.RecordSetServiceGrpc;
import marmot.proto.service.RecordSetServiceGrpc.RecordSetServiceStub;
import marmot.proto.service.UploadRecordRequest;
import marmot.proto.service.UploadRecordResponse;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.Throwables;
import utils.async.AbstractExecution;
import utils.async.Execution;
import utils.async.Executors;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class UploadRecordSet extends AbstractExecution<Long> {
	private static final Logger s_logger = LoggerFactory.getLogger(UploadRecordSet.class);
	
	private static final int SYNC_INTERVAL = 16;
	private static final int CHECK_INTERVAL = 8;
	
	private final RecordSetServiceStub m_stub;
	private final String m_rsetId;
	private final RecordSet m_rset;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private boolean m_done = false;
	@GuardedBy("m_guard") private long m_syncAck = -1;
	@GuardedBy("m_guard") private boolean m_isPeerClosed = false;
	@GuardedBy("m_guard") private Throwable m_failure = null;
	
	public static Execution<Long> start(PBMarmotClient marmot, String rsetId, RecordSet rset) {
		UploadRecordSet upload = new UploadRecordSet(marmot, rsetId, rset);
		Executors.start(upload);
		
		return upload;
	}
	
	public static long run(PBMarmotClient marmot, String rsetId, RecordSet rset) throws Throwable {
		return new UploadRecordSet(marmot, rsetId, rset).executeWork();
	}
	
	UploadRecordSet(PBMarmotClient marmot, String rsetId, RecordSet rset) {
		m_stub = RecordSetServiceGrpc.newStub(marmot.getChannel());
		m_rsetId = rsetId;
		m_rset = rset;
	}

	@Override
	public Long executeWork() throws CancellationException, Throwable {
		UploadRecordResponseHandler resp = new UploadRecordResponseHandler();
		StreamObserver<UploadRecordRequest> channel = m_stub.uploadRecordSet(resp);

		Record record = DefaultRecord.of(m_rset.getRecordSchema());
		try {
			sendHeader(channel);

			long count = 0;
			while ( m_rset.next(record) ) {
				sendRecord(channel, record);
				
				++count;
				if ( count % SYNC_INTERVAL == 0 ) {
					sendSync(channel, count);
					resp.waitSyncAck(count);
				}
				else if ( count % CHECK_INTERVAL == 0 ) {
					resp.checkInterrupt();
				}
			}
			channel.onCompleted();
			
			// 상대방에서 최종적으로 onComplete()를 호출할 때까지 대기한다.
			resp.waitForDone();
			
			return count;
		}
		catch ( CancellationException e ) {
			channel.onCompleted();
			// 상대방에서 최종적으로 onComplete()를 호출할 때까지 대기한다.
			resp.waitForDone();
			
			throw e;
		}
		catch ( ExecutionException e ) {
			channel.onCompleted();
			// 상대방에서 최종적으로 onComplete()를 호출할 때까지 대기한다.
			resp.waitForDone();
			
			throw Throwables.unwrapThrowable(e);
		}
		catch ( Exception e ) {
			sendError(channel, e);
			channel.onCompleted();
			// 상대방에서 최종적으로 onComplete()를 호출할 때까지 대기한다.
			resp.waitForDone();
			
			throw e;
		}
	}
	
	@Override
	public String toString() {
		String closedStr = m_isPeerClosed ? ", peer_closed" : "";
		String errorStr = m_failure != null ? ", failed" : "";
		return String.format("%s[%s]: ack=%d%s%s", getClass(), m_rsetId, m_syncAck,
							closedStr, errorStr);
	}
	
	private void sendHeader(StreamObserver<UploadRecordRequest> channel) {
		UploadRecordRequest header = UploadRecordRequest.newBuilder()
														.setRsetId(m_rsetId)
														.build();
		channel.onNext(header);
	}
	
	private void sendRecord(StreamObserver<UploadRecordRequest> channel, Record record) {
		UploadRecordRequest data = UploadRecordRequest.newBuilder()
													.setRecord(record.toProto())
													.build();
		channel.onNext(data);
	}
	
	private void sendSync(StreamObserver<UploadRecordRequest> channel, long syncNo) {
		UploadRecordRequest eos = UploadRecordRequest.newBuilder()
													.setSync(syncNo)
													.build();
		channel.onNext(eos);
	}
	
	private void sendError(StreamObserver<UploadRecordRequest> channel, Exception e) {
		Throwable cause = (Exception)Throwables.unwrapThrowable(e);
		UploadRecordRequest error = UploadRecordRequest.newBuilder()
													.setError(PBUtils.toErrorProto(cause))
													.build();
		channel.onNext(error);
	}
	
	public class UploadRecordResponseHandler implements StreamObserver<UploadRecordResponse> {
		@Override
		public void onNext(UploadRecordResponse resp) {
			switch ( resp.getEitherCase() ) {
				case SYNC_ACK:
					long ackNo = resp.getSyncAck();
					m_guard.run(() -> m_syncAck = ackNo, true);
					break;
				case ERROR:
					Exception cause = PBUtils.toException(resp.getError());
					m_guard.run(() -> m_failure = cause, true);
					break;
				case RSET_CLOSED:
					m_guard.run(() -> m_isPeerClosed = true, true);
					break;
				default:
					throw new AssertionError();
			}
		}

		@Override
		public void onCompleted() {
			m_guard.run(() -> m_done = true, true);
		}

		@Override
		public void onError(Throwable cause) {
			s_logger.error("unexpected incoming error: {}, class={}",
							cause, UploadRecordResponseHandler.class);
			m_guard.run(() -> { m_failure = cause; m_done = true; }, true);
		}

		@Override
		public String toString() {
			return UploadRecordSet.this.toString();
		}
		
		long waitSyncAck(long syncId) throws InterruptedException,
											CancellationException, ExecutionException {
			return m_guard.awaitUntilAndTryToGet(
				() -> m_syncAck >= syncId || m_done,
				() -> {
					if ( m_isPeerClosed ) {
						throw new CancellationException();
					}
					else if ( m_failure != null ) {
						throw new ExecutionException(m_failure);
					}
					
					return m_syncAck;
				}
			).get();
		}
		
		void waitForDone() throws InterruptedException,
							CancellationException, ExecutionException {
			m_guard.awaitUntilAndTryToGet(
				() -> m_done,
				() -> {
					if ( m_isPeerClosed ) {
						throw new CancellationException();
					}
					else if ( m_failure != null ) {
						throw new ExecutionException(m_failure);
					}
					
					return null;
				}
			).get();
		}
		
		void checkInterrupt() throws CancellationException, ExecutionException {
			m_guard.tryToRun(() -> {
				if ( m_isPeerClosed ) {
					throw new CancellationException();
				}
				else if ( m_failure != null ) {
					throw new ExecutionException(m_failure);
				}
			}).get();
		}
	}
}
