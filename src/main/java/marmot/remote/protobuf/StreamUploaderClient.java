package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Objects;

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
import utils.async.AbstractExecution;
import utils.async.CancellableWork;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamUploaderClient extends AbstractExecution<ByteString>
									implements CancellableWork, StreamObserver<UpChunkResponse> {
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	private static final int SYNC_INTERVAL = 4;
	
	private final InputStream m_stream;
	private StreamObserver<UpChunkRequest> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private int m_sync = 0;
	
	protected StreamUploaderClient(InputStream stream) {
		Objects.requireNonNull(stream, "Stream to upload");
		
		m_stream = stream;
		setLogger(LoggerFactory.getLogger(StreamUploaderClient.class));
	}
	
	void setOutputChannel(StreamObserver<UpChunkRequest> channel) {
		Objects.requireNonNull(channel, "Upload stream channel");

		m_channel = channel;
	}
	
	void sendRequest(ByteString req) {
		Preconditions.checkState(m_channel != null, "Upload stream channel has not been set");
		
		m_channel.onNext(UpChunkRequest.newBuilder().setHeader(req).build());
	}

	@Override
	public ByteString executeWork() throws Exception {
		Preconditions.checkState(m_channel != null, "Upload stream channel has not been set");
		
		try {
			int chunkCount = 0;
			while ( true ) {
				// chunk를 보내는 과정 중에 자체적으로 또는 상대방쪽에서
				// 오류가 발생되어있을 수 있으니 확인한다.
				if ( !isRunning() ) {
					break;
				}
				
				try {
					LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
					ByteString chunk = ByteString.readFrom(chunkedStream);
					if ( chunk.isEmpty() ) {
						break;
					}
					
					m_channel.onNext(UpChunkRequest.newBuilder()
													.setChunk(chunk)
													.build());
					++chunkCount;
					if ( getLogger().isDebugEnabled() ) {
						getLogger().debug("sent CHUNK[idx={}, size={}]", chunkCount, chunk.size());
					}
				}
				catch ( Exception e ) {
					Throwable cause = Throwables.unwrapThrowable(e);
					m_channel.onNext(UpChunkRequest.newBuilder()
													.setError(PBUtils.toErrorProto(cause))
													.build());
					throw e;
				}
				
				if ( (chunkCount % SYNC_INTERVAL) == 0 ) {
					sync(chunkCount, chunkCount - SYNC_INTERVAL);
				}
			}
			
			// 마지막 chunk에 대한 sync를 보내고, sync-back을 대기하고
			// End-of-Stream 메시지를 보낸다.
			if ( m_guard.get(()->m_sync) < chunkCount ) {
				sync(chunkCount, chunkCount);
			}
			m_channel.onNext(UpChunkRequest.newBuilder()
											.setEos(PBUtils.VOID)
											.build());
			getLogger().info("sent END_OF_STREAM");
			
			return get();
		}
		finally {
			m_channel.onCompleted();
			
			IOUtils.closeQuietly(m_stream);
		}
	}

	@Override
	public boolean cancelWork() {
		return true;
	}

	@Override
	public void onNext(UpChunkResponse resp) {
		switch ( resp.getEitherCase() ) {
			case SYNC_BACK:
				if ( getLogger().isDebugEnabled() ) {
					getLogger().debug("received SYNC_BACK[{}]", resp.getSyncBack());
				}
				m_guard.run(() -> m_sync = resp.getSyncBack(), true);
				break;
			case RESULT:
				ByteString result = resp.getResult();
				getLogger().debug("received RESULT[size={}]", result.size());
				
				notifyCompleted(result);
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
		getLogger().warn("received SYSTEM_ERROR[cause={}]", cause);
		cancel();
	}

	@Override
	public void onCompleted() {
		getLogger().debug("received COMPLETE");
		cancel();
	}
	
	private int sync(int sync, int expectedSyncBack) throws InterruptedException {
		m_channel.onNext(UpChunkRequest.newBuilder()
										.setSync(sync)
										.build());
		if ( getLogger().isDebugEnabled() ) {
			getLogger().debug("sent SYNC[{}] & wait for SYNC[{}]", sync, expectedSyncBack);
		}

		return m_guard.awaitUntilAndGet(() -> m_sync >= sync, () -> m_sync);
	}
}
