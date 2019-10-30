package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.RecordSet;
import marmot.io.MarmotFileNotFoundException;
import marmot.proto.LongProto;
import marmot.proto.service.CopyToHdfsFileRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.FileServiceGrpc;
import marmot.proto.service.FileServiceGrpc.FileServiceBlockingStub;
import marmot.proto.service.FileServiceGrpc.FileServiceStub;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBUtils;
import marmot.rset.PBInputStreamRecordSet;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;
import utils.func.FOption;
import utils.io.Lz4Compressions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBFileServiceProxy {
	private static final Logger s_logger = LoggerFactory.getLogger(PBFileServiceProxy.class);
	private static final long REPORT_INTERVAL = UnitUtils.parseByteSize("512mb");
	
	private final PBMarmotClient m_marmot;
	private final FileServiceStub m_stub;
	private final FileServiceBlockingStub m_blockingStub;

	PBFileServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
		m_marmot = marmot;
		m_blockingStub = FileServiceGrpc.newBlockingStub(channel);
		m_stub = FileServiceGrpc.newStub(channel);
	}
	
	public RecordSet readMarmotFile(String path) throws MarmotFileNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<DownChunkResponse> channel = m_stub.readMarmotFile(downloader);
		InputStream is = downloader.start(PBUtils.toStringProto(path).toByteString(), channel);
		
		return PBInputStreamRecordSet.from(is);
	}

	public void deleteHdfsFile(String path) throws IOException {
		try {
			VoidResponse resp = m_blockingStub.deleteHdfsFile(PBUtils.toStringProto(path));
			PBUtils.handle(resp);
		}
		catch ( Exception e ) {
			Throwables.throwIfInstanceOf(Throwables.unwrapThrowable(e), IOException.class);
			throw Throwables.toRuntimeException(e);
		}
	}

	public long copyToHdfsFile(String path, InputStream stream, FOption<Long> blockSize,
								FOption<String> codecName)
		throws IOException {
		try {
			if ( m_marmot.useCompression() ) {
				stream = Lz4Compressions.compress(stream);
			}
			
			StreamUploadSender uploader = new StreamUploadSender(stream) {
				@Override
				protected ByteString getHeader() throws Exception {
					CopyToHdfsFileRequest.Builder hbuilder = CopyToHdfsFileRequest.newBuilder()
																.setPath(PBUtils.toStringProto(path))
																.setUseCompression(m_marmot.useCompression());
					blockSize.ifPresent(sz -> hbuilder.setBlockSize(sz));
					codecName.ifPresent(hbuilder::setCompressionCodecName);
					CopyToHdfsFileRequest req = hbuilder.build();
					return req.toByteString();
				}
			};
			StreamObserver<UpChunkRequest> channel = m_stub.copyToHdfsFile(uploader);
			uploader.setChannel(channel);
			uploader.start();
			
			ByteString ret = uploader.get();
			return LongProto.parseFrom(ret).getValue();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	private void logUploadProgress(String path, long totalUploaded, StopWatch watch) {
		if ( s_logger.isInfoEnabled() ) {
			String size = UnitUtils.toByteSizeString(totalUploaded);
			long velo = (watch.getElapsedInSeconds() > 0)
						? totalUploaded / watch.getElapsedInSeconds() : -1;
			String veloStr = UnitUtils.toByteSizeString(velo);
			String msg = String.format("path=%s, total=%s, velo=%s/s, elapsed=%s",
										path, size, veloStr, watch.getElapsedSecondString());
			s_logger.info(msg);
		}
	}
}
