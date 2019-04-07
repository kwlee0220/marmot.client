package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.protobuf.LimitedInputStream;
import marmot.protobuf.PBUtils;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.Throwables;
import utils.async.AbstractThreadedExecution;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
abstract class StreamUploadSender extends AbstractThreadedExecution<ByteString>
									implements StreamObserver<UpChunkResponse> {
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	private static final int SYNC_INTERVAL = 4;
	private static final int TIMEOUT = 5;		// 5s
	
	private final InputStream m_stream;
	private StreamObserver<UpChunkRequest> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private int m_sync = 0;
	@GuardedBy("m_guard") private ByteString m_result = null;
	@GuardedBy("m_guard") private boolean m_serverClosed = false;
	
	abstract protected ByteString getHeader() throws Exception;
	
	protected StreamUploadSender(InputStream stream) {
		Objects.requireNonNull(stream, "Stream to upload");
		
		m_stream = stream;
		setLogger(LoggerFactory.getLogger(StreamUploadSender.class));
	}
	
	void setChannel(StreamObserver<UpChunkRequest> channel) {
		Objects.requireNonNull(channel, "Upload stream channel");

		m_channel = channel;
	}

	@Override
	public ByteString executeWork() throws Exception {
		Preconditions.checkState(m_channel != null, "Upload stream channel has not been set");
		
		try {
			m_channel.onNext(HEADER());
			
			int chunkCount = 0;
			while ( isRunning() ) {
				LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
				ByteString chunk = ByteString.readFrom(chunkedStream);
				if ( chunk.isEmpty() ) {
					// 마지막 chunk에 대한 sync를 보내고, sync-back을 대기하고
					// End-of-Stream 메시지를 보낸다.
					if ( m_guard.get(()->m_sync) < chunkCount ) {
						sync(chunkCount, chunkCount);
					}
					m_channel.onNext(EOS);
					getLogger().debug("sent END_OF_STREAM");
					
					// 연산이 종료되면 connection 자체가 종료되기 때문에, 서버측에서 'half-close' 될 수 있기
					// 때문에, 단순히 result만 기다리는 것이 아니라, 'onCompleted()'가 호출될 때까지 기다린다.
					return m_guard.awaitUntilAndGet(() -> m_result != null, () -> m_result,
													TIMEOUT, TimeUnit.SECONDS);
				}
				
				m_channel.onNext(UpChunkRequest.newBuilder().setChunk(chunk).build());
				++chunkCount;
				getLogger().trace("sent CHUNK[idx={}, size={}]", chunkCount, chunk.size());
				
				if ( (chunkCount % SYNC_INTERVAL) == 0 ) {
					sync(chunkCount, chunkCount - SYNC_INTERVAL);
				}
			}
			
			return m_guard.get(() -> m_result);
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			m_channel.onNext(ERROR("" + cause));
			
			throw e;
		}
		finally {
			m_channel.onCompleted();
			
			IOUtils.closeQuietly(m_stream);
		}
	}

	@Override
	public void onNext(UpChunkResponse resp) {
		switch ( resp.getEitherCase() ) {
			case SYNC_BACK:
				getLogger().debug("received SYNC_BACK[{}]", resp.getSyncBack());
				
				m_guard.run(() -> m_sync = resp.getSyncBack(), true);
				break;
			case RESULT:
				// 스트림의 모든 chunk를 다 보내기 전에 result가 올 수 있기 때문에
				// 모든 chunk를 다보내고 result가 도착해야만 uploader를 종료시킬 수 있음.
				ByteString result = resp.getResult();
				m_guard.run(() -> m_result = result, true);
				getLogger().debug("received RESULT: {}", result);
				break;
			case ERROR:
				Exception cause = PBUtils.toException(resp.getError());
				getLogger().info("received PEER_ERROR[cause={}]", ""+cause);
				
				notifyFailed(cause);
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		getLogger().warn("received SYSTEM_ERROR[cause=" + cause + "]");
		
		notifyFailed(cause);
	}

	@Override
	public void onCompleted() {
		getLogger().debug("received SERVER_COMPLETE");
		m_guard.run(() -> m_serverClosed = true, true);
	}
	
	private int sync(int sync, int expectedSyncBack)
		throws InterruptedException, TimeoutException {
		m_channel.onNext(SYNC(sync));
		getLogger().debug("sent SYNC[{}] & wait for SYNC[{}]", sync, expectedSyncBack);

		return m_guard.awaitUntilAndGet(() -> m_sync >= sync, () -> m_sync,
										TIMEOUT, TimeUnit.SECONDS);
	}
	
	private UpChunkRequest HEADER() throws Exception {
		return UpChunkRequest.newBuilder().setHeader(getHeader()).build();
	}
	
	private UpChunkRequest ERROR(String msg) {
		PBRemoteException cause = new PBRemoteException(msg); 
		return UpChunkRequest.newBuilder()
							.setError(PBUtils.toErrorProto(cause))
							.build();
	}
	
	private UpChunkRequest SYNC(int sync) {
		return UpChunkRequest.newBuilder().setSync(sync).build();
	}
	private static final UpChunkRequest EOS = UpChunkRequest.newBuilder()
															.setEos(PBUtils.VOID)
															.build();
}
