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
 * @author Kang-Woo Lee (ETRI)
 */
class StreamDownloaderClient implements StreamObserver<DownChunkResponse>, LoggerSettable {
	private final ChunkInputStream m_stream;
	private StreamObserver<DownChunkRequest> m_channel;
	private Logger m_logger = LoggerFactory.getLogger(StreamDownloaderClient.class);
	
	StreamDownloaderClient() {
		m_stream = ChunkInputStream.create(4);
	}
	
	void setOutputChannel(StreamObserver<DownChunkRequest> channel) {
		Objects.requireNonNull(channel, "download stream channel");
		
		m_channel = channel;
	}

	InputStream start(ByteString req) {
		Preconditions.checkState(m_channel != null, "Download stream request channel is not set");

		m_channel.onNext(DownChunkRequest.newBuilder().setHeader(req).build());
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
	public void onNext(DownChunkResponse resp) {
		switch ( resp.getEitherCase() ) {
			case CHUNK:
				try {
					ByteString chunk = resp.getChunk();
					if ( getLogger().isDebugEnabled() ) {
						getLogger().debug("received CHUNK[size={}]", chunk.size());
					}
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
					m_channel.onNext(DownChunkRequest.newBuilder()
													.setSyncBack(sync)
													.build());
				}
				break;
			case EOS:
				getLogger().debug("received END_OF_STREAM");
				m_stream.endOfSupply();
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
		getLogger().warn("received SYSTEM_ERROR[cause={}]", cause);
		m_stream.endOfSupply(Throwables.unwrapThrowable(cause));
	}
	
	private void sendError(Throwable cause) {
		m_channel.onNext(DownChunkRequest.newBuilder()
										.setError(PBUtils.toErrorProto(cause))
										.build());
	}
}