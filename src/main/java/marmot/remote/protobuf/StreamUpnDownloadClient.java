package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Objects;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.UpRequestDownResponse;
import marmot.proto.service.UpResponseDownRequest;
import marmot.protobuf.PBUtils;
import marmot.remote.protobuf.StreamObservers.ClientUpDownChannel;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
abstract class StreamUpnDownloadClient implements StreamObserver<UpResponseDownRequest> {
	private final StreamUploaderSender m_uploader;
	private final StreamDownloadReceiver m_downloader;
	
	abstract protected ByteString getHeader() throws Exception;
	
	StreamUpnDownloadClient(InputStream stream) {
		Objects.requireNonNull(stream, "Stream to upload");
		
		m_uploader = new StreamUploaderSender(stream) {
			@Override
			protected ByteString getHeader() throws Exception {
				return StreamUpnDownloadClient.this.getHeader();
			}
		};
		m_downloader = new StreamDownloadReceiver();
	}
	
	InputStream upAndDownload(StreamObserver<UpRequestDownResponse> channel) {
		Objects.requireNonNull(channel, "UpRequestDownResponse stream channel");
		
		ClientUpDownChannel upDown = StreamObservers.getClientUpDownChannel(channel);

		m_uploader.setChannel(upDown.getUploadChannel());
		m_uploader.start();
		
		return m_downloader.receive(upDown.getDownloadChannel());
	}

	@Override
	public synchronized void onNext(UpResponseDownRequest msg) {
		switch ( msg.getEitherCase() ) {
			case DOWN_REQ:
				m_downloader.onNext(msg.getDownReq());
				break;
			case UP_RESP:
				m_uploader.onNext(msg.getUpResp());
				break;
			case UP_CLOSED:
				m_uploader.onCompleted();
				break;
			case DOWN_CLOSED:
				m_downloader.onCompleted();
				break;
			case UP_ERROR:
				m_uploader.onError(PBUtils.toException(msg.getUpError()));
				break;
			case DOWN_ERROR:
				m_downloader.onError(PBUtils.toException(msg.getDownError()));
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		m_downloader.onError(cause);
		m_uploader.onError(cause);
	}

	@Override
	public void onCompleted() {
		m_uploader.onCompleted();
		m_downloader.onCompleted();
	}
}
