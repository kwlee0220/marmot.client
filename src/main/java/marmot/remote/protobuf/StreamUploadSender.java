package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.protobuf.PBUtils;
import utils.Throwables;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.Guard;
import utils.io.IOUtils;
import utils.io.LimitedInputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
abstract class StreamUploadSender extends AbstractThreadedExecution<ByteString>
									implements StreamObserver<UpChunkResponse> {
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	private static final int SYNC_INTERVAL = 4;
	private static final int TIMEOUT = 30;		// 30s
	
	private final InputStream m_stream;
	private StreamObserver<UpChunkRequest> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private int m_sync = 0;
	@GuardedBy("m_guard") private ByteString m_reply = null;
	@GuardedBy("m_guard") private boolean m_serverClosed = false;
	@GuardedBy("m_guard") private Exception m_error = null;
	@GuardedBy("m_guard") private boolean m_completed = false;
	
	abstract protected ByteString getHeader() throws Exception;
	
	protected StreamUploadSender(InputStream stream) {
		Utilities.checkNotNullArgument(stream, "Stream to upload");
		
		m_stream = stream;
		setLogger(LoggerFactory.getLogger(StreamUploadSender.class));
	}
	
	void setChannel(StreamObserver<UpChunkRequest> channel) {
		Utilities.checkNotNullArgument(channel, "Upload stream channel");

		m_channel = channel;
	}

	@Override
	public ByteString executeWork() throws Exception {
		Preconditions.checkState(m_channel != null, "Upload stream channel has not been set");
		
		try {
			ByteString hdr = getHeader();
			m_channel.onNext(HEADER(hdr));
			getLogger().debug("sent HEADER: {}", hdr);
			
			int chunkCount = 0;
			while ( isRunning() ) {
				LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
				ByteString chunk = ByteString.readFrom(chunkedStream);
				if ( chunk.isEmpty() ) {
					// 마지막 chunk에 대한 sync를 보내고, sync-back을 대기한다.
					if ( m_guard.get(()->m_sync) < chunkCount ) {
						if ( sync(chunkCount, chunkCount) < 0 ) {
							break;
						}
					}
					
					//
					// End-of-Stream 메시지를 보내고, 서버쯕에서 connection을 끊기를 대기한다
					// 여기서 먼저 connection을 끊으면 서버측에서 'half-close' 될 수 있기
					// 때문에, 단순히 result만 기다리는 것이 아니라, 'onCompleted()'가
					// 호출될 때까지 기다린다.
					//
					m_channel.onNext(EOS);
					m_guard.runAndSignalAll(() -> m_completed = true);
					getLogger().debug("sent END_OF_STREAM");

					m_guard.awaitUntil(() -> m_serverClosed || m_error != null,
										TIMEOUT, TimeUnit.SECONDS);
					break;
				}
				
				m_channel.onNext(UpChunkRequest.newBuilder().setChunk(chunk).build());
				++chunkCount;
				getLogger().trace("sent CHUNK[idx={}, size={}]", chunkCount, chunk.size());
				
				if ( (chunkCount % SYNC_INTERVAL) == 0 ) {
					// 세가지 가능성 고려할 것
					//	1. sync-back을 성공적으로 전송
					//	2. server측에서 오류 전송
					//	3. server측에서 connection 단절
					if ( sync(chunkCount, chunkCount - SYNC_INTERVAL) < 0 ) {
						break;
					}
				}
			}
			
			// 여기에 올 수 있는 세가지 경우
			//  1. 모든 작업을 성공적으로 마친 경우
			//  2. server측에서 오류 전송
			//  3. server측에서 connection 단절
			
			m_guard.lock();
			try {
				if ( m_error != null ) {
					throw m_error;
				}
				if ( m_reply != null ) {
					return m_reply;
				}
				else {
					m_error = new PBStreamClosedException("service disconnection");
					throw m_error;
				}
			}
			finally {
				m_guard.unlock();
			}
		}
		catch ( Exception e ) {
			if ( m_guard.get(() -> m_error == null) ) {
				Throwable cause = Throwables.unwrapThrowable(e);
				m_channel.onNext(ERROR("" + cause));
			}
			
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
				
				m_guard.runAndSignalAll(() -> m_sync = resp.getSyncBack());
				break;
			case RESULT:
				// 스트림의 모든 chunk를 다 보내기 전에 result가 올 수 있기 때문에
				// 모든 chunk를 다보내고 result가 도착해야만 uploader를 종료시킬 수 있음.
				ByteString result = resp.getResult();
				m_guard.runAndSignalAll(() -> m_reply = result);
				getLogger().debug("received RESULT: {}", result);
				break;
			case ERROR:
				Exception cause = PBUtils.toException(resp.getError());
				getLogger().info("received PEER_ERROR[cause={}]", ""+cause);

				m_guard.runAndSignalAll(() -> m_error = cause);
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
		m_guard.runAndSignalAll(() -> m_serverClosed = true);
	}
	
	private int sync(int sync, int expectedSyncBack)
		throws InterruptedException, TimeoutException {
		m_channel.onNext(SYNC(sync));
		getLogger().debug("sent SYNC[{}] & wait for SYNC[{}]", sync, expectedSyncBack);
		
		Date due = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TIMEOUT));
		m_guard.lock();
		try {
			while ( true ) {
				if ( m_sync >= sync ) {			// sync에 대한 응답이 온 경우.
					return m_sync;
				}
				else if ( m_error != null || m_serverClosed ) {
					return -1;
				}
				if ( !m_guard.awaitUntil(due) ) {
					throw new TimeoutException();
				}
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void awaitServiceClosed(long timeout)
		throws InterruptedException, TimeoutException, Exception {
		Date due = new Date(System.currentTimeMillis() + timeout);
		
		m_guard.lock();
		try {
			while ( !(m_serverClosed || m_error != null) ) {
				if ( !m_guard.awaitUntil(due) ) {
					throw new TimeoutException();
				}
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private UpChunkRequest HEADER(ByteString hdr) throws Exception {
		return UpChunkRequest.newBuilder().setHeader(hdr).build();
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
