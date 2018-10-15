package marmot.remote.protobuf;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.vavr.control.Option;
import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.proto.service.NextBulkRequest;
import marmot.proto.service.RecordSetServiceGrpc.RecordSetServiceStub;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBUtils;
import marmot.protobuf.SingleValueObserver;
import marmot.rset.AbstractRecordSet;
import marmot.rset.PipedRecordSet;
import marmot.rset.RecordSets;
import utils.async.ExecutableHandle;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ServerRecordSetPuller extends ExecutableHandle<Void> {
	private final String m_rsetId;
	private final PipedRecordSet m_queue;
	private final RecordSetServiceStub m_stub;
	
	public ServerRecordSetPuller(String rsetId, RecordSchema schema,
								RecordSetServiceStub stub, int bulkSize) {
		m_rsetId = rsetId;
		m_queue = RecordSets.pipe(schema, bulkSize);
		m_stub = stub;
	}
	
	public RecordSet getConsumerRecordSet() {
		return m_consumerRecordSet;
	}
	
	@Override
	public Void executeWork() throws CancellationException, ExecutionException {
		try {
			while ( pullRecordBulk() ) {
				if ( m_queue.isConsumerClosed() ) {	// m_queue가 이미 close된 경우.
					throw new CancellationException();
				}
			}
			m_queue.endOfSupply();
			
			return null;
		}
		catch ( InterruptedException e ) {
			throw new CancellationException();
		}
		catch ( ExecutionException e ) {
			m_queue.endOfSupply(e);
			
			throw e;
		}
	}
	
	private boolean pullRecordBulk() throws InterruptedException, ExecutionException {
		NextBulkRequest req = NextBulkRequest.newBuilder()
											.setRsetId(m_rsetId)
											.setBulkSize(m_queue.getMaxQueueLength())
											.build();
		RecordBulkReceiver receiver = new RecordBulkReceiver(m_queue);
		m_stub.getNextRecordBulk(req, receiver);
		
		return receiver.waitForDone();
	}
	
	private final RecordSet m_consumerRecordSet = new AbstractRecordSet() {
		@Override
		protected void closeInGuard() {
			// 먼저 queue를 close시켜 더이상 서버에서는 전달되는 레코드를 무시하도록 한다.
			m_queue.closeQuietly();
			
			// queue가 완전히 close될 때까지 대기한다.
			// queue가 다른 
			// 너무 오래동안 기리지 않도록 하기 위해 3초만 대기한다.
			try {
				m_queue.waitForFullyClosed(3, TimeUnit.SECONDS);
			}
			catch ( InterruptedException ignored ) { }
			
			// RecordPullSupplier 쓰레드가 종료할 때까지 대기한다.
			ServerRecordSetPuller supplier = ServerRecordSetPuller.this;
			Try.run(supplier::waitForDone);
			
			// 서버측 RecordSet를 close시킨다.
			SingleValueObserver<VoidResponse> ret = SingleValueObserver.create();
			m_stub.close(PBUtils.toStringProto(m_rsetId), ret);
			Try.run(() -> ret.await());
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_queue.getRecordSchema();
		}
		
		@Override
		public Option<Record> nextCopy() {
			return m_queue.nextCopy();
		}
	};
}
