package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.protobuf.ChunkInputStream;
import marmot.protobuf.PBUtils;
import utils.LoggerSettable;
import utils.Throwables;


/**
 * 
 * 사용자의 request 메시지가 있는 경우 (즉, receive()에 req가 전달된 경우),
 * 해당 메시지를 전송하는 것으로 시작됨.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class StreamDownloadReceiver implements StreamObserver<DownChunkRequest>, LoggerSettable {
	private final ChunkInputStream m_stream;
	private StreamObserver<DownChunkResponse> m_channel;
	private Logger m_logger = LoggerFactory.getLogger(StreamDownloadReceiver.class);
	
	StreamDownloadReceiver() {
		m_stream = ChunkInputStream.create(4);
	}

	InputStream receive(ByteString req, StreamObserver<DownChunkResponse> channel) {
		Objects.requireNonNull(channel, "download stream channel");

		m_channel = channel;
		m_channel.onNext(DownChunkResponse.newBuilder().setHeader(req).build());
		return m_stream;
	}

	InputStream receive(StreamObserver<DownChunkResponse> channel) {
		Objects.requireNonNull(channel, "download stream channel");

		m_channel = channel;
		return m_stream;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	@Override
	public void onNext(DownChunkRequest resp) {
		switch ( resp.getEitherCase() ) {
			case CHUNK:
				try {
					ByteString chunk = resp.getChunk();
					getLogger().trace("received CHUNK[size={}]", chunk.size());
					m_stream.supply(resp.getChunk());
				}
				catch ( PBStreamClosedException e ) {
					sendError(e);
				}
				catch ( InterruptedException e ) {
					sendError(e);
					
					m_stream.endOfSupply(e);
					getLogger().info("detect STREAM ERROR[cause={}]", e.toString());
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
			case ERROR:
				Exception cause = PBUtils.toException(resp.getError());
				getLogger().info("received PEER_ERROR[cause={}]", cause.toString());
				m_stream.endOfSupply(cause);
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onCompleted() {
		getLogger().debug("received COMPLETE");
		m_stream.endOfSupply();
	}
	
	@Override
	public void onError(Throwable cause) {
		getLogger().warn("received SYSTEM_ERROR[cause=" + cause + "]");
		m_stream.endOfSupply(Throwables.unwrapThrowable(cause));
	}
	
	private void sendError(Throwable cause) {
		m_channel.onNext(DownChunkResponse.newBuilder()
										.setError(PBUtils.toErrorProto(cause))
										.build());
	}
}