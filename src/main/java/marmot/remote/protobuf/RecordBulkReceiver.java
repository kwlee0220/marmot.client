package marmot.remote.protobuf;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import marmot.Record;
import marmot.proto.service.RecordBulkResponse;
import marmot.protobuf.PBUtils;
import marmot.rset.PipedRecordSet;
import marmot.support.DefaultRecord;
import net.jcip.annotations.GuardedBy;
import utils.Guard;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class RecordBulkReceiver implements StreamObserver<RecordBulkResponse> {
	private static final Logger s_logger = LoggerFactory.getLogger(RecordBulkReceiver.class);
	
	private final PipedRecordSet m_queue;
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private boolean m_done = false;
	@GuardedBy("m_guard") private boolean m_eos = false;
	@GuardedBy("m_guard") private Throwable m_failure = null;
	
	RecordBulkReceiver(PipedRecordSet queue) {
		m_queue = queue;
	}
	
	public boolean waitForDone() throws InterruptedException, ExecutionException {
		return m_guard.awaitUntilAndTryToGet(() -> m_done, () -> {
			if ( m_failure == null ) {
				return !m_eos;
			}
			else {
				throw new ExecutionException(m_failure);
			}
		}).get();
	}
	
	@Override
	public void onNext(RecordBulkResponse value) {
		switch ( value.getEitherCase() ) {
			case RECORD:
				Record rec = DefaultRecord.fromProto(m_queue.getRecordSchema(), value.getRecord());
				if ( !m_queue.supply(rec) ) {
					// 클라이언트(consumer)쪽에서 더 이상 데이터가 필요없다고 한 경우. 무시한다.
				}
				break;
			case END_OF_STREAM:
				m_guard.runAndSignal(() -> m_eos = true);
				break;
			case ERROR:
				Exception error = PBUtils.toException(value.getError());
				m_guard.runAndSignal(() -> m_failure = error);
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		s_logger.error("unexpected incoming error: {}, class={}",
						cause, RecordBulkReceiver.class);
		m_guard.runAndSignal(() -> { m_done = true; m_failure = cause; });
	}

	@Override
	public void onCompleted() {
		m_guard.runAndSignal(() -> m_done = true);
	}
}
