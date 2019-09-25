package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.protobuf.PBUtils;
import marmot.protobuf.SuppliableInputStream;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.CancellableWork;
import utils.async.EventDrivenExecution;


/**
 * 
 * 사용자의 request 메시지가 있는 경우 (즉, receive()에 req가 전달된 경우),
 * 해당 메시지를 전송하는 것으로 시작됨.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class StreamDownloadReceiver extends EventDrivenExecution<Void>
							implements StreamObserver<DownChunkRequest>, CancellableWork {
	private final SuppliableInputStream m_stream;
	private StreamObserver<DownChunkResponse> m_channel;

	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.DOWNLOADING;
	private static enum State {
		DOWNLOADING,
		COMPLETED,
		CANCELLING,
		CANCELLED,
	}
	
	StreamDownloadReceiver() {
		m_stream = new CancellableInputStream(4);
		
		setLogger(LoggerFactory.getLogger(StreamDownloadReceiver.class));
	}
	
	private class CancellableInputStream extends SuppliableInputStream {
		protected CancellableInputStream(int chunkQLength) {
			super(chunkQLength);
		}
		
		@Override
		public void close() throws IOException {
			StreamDownloadReceiver.this.cancel(true);
			super.close();
		}
	}

	InputStream start(ByteString req, StreamObserver<DownChunkResponse> channel) {
		Utilities.checkNotNullArgument(req, "download-stream-consumer request");
		Utilities.checkNotNullArgument(channel, "download-stream channel");

		m_channel = channel;
		m_channel.onNext(DownChunkResponse.newBuilder().setHeader(req).build());
		notifyStarted();
		
		return m_stream;
	}

	InputStream start(StreamObserver<DownChunkResponse> channel) {
		Utilities.checkNotNullArgument(channel, "download-stream channel");

		m_channel = channel;
		notifyStarted();
		
		return m_stream;
	}

	@Override
	public boolean notifyFailed(Throwable cause) {
		m_stream.endOfSupply(cause);
		
		return super.notifyFailed(cause);
	}

	private static final long CANCELLING_TIMEOUT = UnitUtils.parseDuration("50s");
	@Override
	public boolean cancelWork() {
		m_guard.lock();
		try {
			if ( m_state == State.DOWNLOADING ) {
				m_state = State.CANCELLING;
				m_guard.signalAll();
				
				m_stream.endOfSupply();
				
				// 'CANCEL' 메시지를 보내고, 서버측에서 종료할 때까지 대기한다.
				getLogger().debug("send CANCEL");
				DownChunkResponse cancel = DownChunkResponse.newBuilder().setCancel(true).build();
				m_channel.onNext(cancel);
			}

			Date due = new Date(System.currentTimeMillis() + CANCELLING_TIMEOUT);
			while ( m_state == State.CANCELLING ) {
				try {
					if ( !m_guard.awaitUntil(due) ) {
						break;
					}
				}
				catch ( InterruptedException e ) {
					return false;
				}
			}
			
			return m_state == State.CANCELLED;
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public void onNext(DownChunkRequest resp) {
		switch ( resp.getEitherCase() ) {
			case CHUNK:
				try {
					ByteString chunk = resp.getChunk();
					getLogger().trace("received CHUNK[size={}]", chunk.size());
					
					m_stream.supply(chunk);
				}
				catch ( PBStreamClosedException e ) {
					if ( isRunning() ) {
						getLogger().info("detect consumer has closed the stream");
						
						// download된 stream 사용자가 stream을 이미 close시킨 경우.
//						m_stream.endOfSupply();
						notifyCancelled();
						sendError(e);
					}
				}
				catch ( Exception e ) {
					Throwable cause = Throwables.unwrapThrowable(e);
					getLogger().info("detect STREAM ERROR[cause={}]",cause);

					sendError(e);
					notifyFailed(e);
				}
				break;
			case SYNC:
				int sync = resp.getSync();
				getLogger().debug("received SYNC[{}]", sync);
				
				if ( !m_stream.isClosed() && m_guard.get(() -> m_state == State.DOWNLOADING) ) {
					getLogger().debug("send SYNC_BACK[{}]", sync);
					m_channel.onNext(DownChunkResponse.newBuilder()
													.setSyncBack(sync)
													.build());
				}
				break;
			case EOS:
				onEoSReceived();
				break;
			case ERROR:
				Exception cause = PBUtils.toException(resp.getError());
				getLogger().info("received PEER_ERROR[cause={}]", cause.toString());
				m_stream.endOfSupply(cause);
				
				notifyFailed(cause);
				break;
			default:
				throw new AssertionError();
		}
	}
	
	private void onEoSReceived() {
		getLogger().debug("received END_OF_STREAM");
		
		m_guard.lock();
		try {
			if ( m_state == State.DOWNLOADING ) {
				m_stream.endOfSupply();
				notifyCompleted(null);
				
				m_state = State.COMPLETED;
			}
			
			m_guard.signalAll();
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public void onCompleted() {
		State state = m_guard.get(() -> {
			if ( m_state == State.CANCELLING ) {
				m_state = State.CANCELLED;
			}
			
			return m_state;
		}, true);
		if ( state == State.DOWNLOADING ) {
			Throwable cause = new IOException("Peer has broken the pipe");
			m_stream.endOfSupply(cause);
			notifyFailed(cause);
		}
	}
	
	@Override
	public void onError(Throwable cause) {
		getLogger().warn("received SYSTEM_ERROR[cause=" + cause + "]");
		if ( !isDone() ) {
			cause = new IOException("Peer has broken the pipe");
			m_stream.endOfSupply(cause);
			notifyFailed(cause);
		}
	}
	
	private void sendError(Throwable cause) {
		m_channel.onNext(DownChunkResponse.newBuilder()
										.setError(PBUtils.toErrorProto(cause))
										.build());
	}
}