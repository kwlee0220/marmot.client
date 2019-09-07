package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.protobuf.PBUtils;
import marmot.protobuf.SuppliableInputStream;
import utils.Throwables;
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
	
	StreamDownloadReceiver() {
		m_stream = SuppliableInputStream.create(4);
		
		setLogger(LoggerFactory.getLogger(StreamDownloadReceiver.class));
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

	@Override
	public boolean cancelWork() {
		m_stream.endOfSupply();
		
		DownChunkResponse cancel = DownChunkResponse.newBuilder().setCancel(true).build();
		m_channel.onNext(cancel);
		m_channel.onCompleted();
		
		return true;
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
				
				if ( !m_stream.isClosed() ) {
					getLogger().debug("send SYNC_BACK[{}]", sync);
					m_channel.onNext(DownChunkResponse.newBuilder()
													.setSyncBack(sync)
													.build());
				}
				break;
			case EOS:
				getLogger().debug("received END_OF_STREAM");
				m_stream.endOfSupply();
				
				notifyCompleted(null);
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

	@Override
	public void onCompleted() {
		if ( !isDone() ) {
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