package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import marmot.RecordSet;
import marmot.io.MarmotFileNotFoundException;
import marmot.proto.service.CopyToHdfsFileRequest;
import marmot.proto.service.CopyToHdfsFileRequest.HeaderProto;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.FileServiceGrpc;
import marmot.proto.service.FileServiceGrpc.FileServiceBlockingStub;
import marmot.proto.service.FileServiceGrpc.FileServiceStub;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBUtils;
import marmot.protobuf.SingleValueObserver;
import marmot.rset.PBInputStreamRecordSet;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBFileServiceProxy {
	private static final Logger s_logger = LoggerFactory.getLogger(PBFileServiceProxy.class);
	private static final long REPORT_INTERVAL = UnitUtils.parseByteSize("512mb");
	
//	private final PBMarmotClient m_marmot;
	private final FileServiceStub m_stub;
	private final FileServiceBlockingStub m_blockingStub;

	PBFileServiceProxy(PBMarmotClient marmot, ManagedChannel channel) {
//		m_marmot = marmot;
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

	public void copyToHdfsFile(String path, Iterator<byte[]> blocks, FOption<Long> blockSize)
		throws IOException {
		StopWatch watch = StopWatch.start();
		int nblocks = 0;
		
		SingleValueObserver<VoidResponse> value = new SingleValueObserver<>();
		CallStreamObserver<CopyToHdfsFileRequest> supplier
			= (CallStreamObserver<CopyToHdfsFileRequest>)m_stub.copyToHdfsFile(value);
		try {
			HeaderProto.Builder hbuilder = HeaderProto.newBuilder()
													.setPath(PBUtils.toStringProto(path));
			blockSize.ifPresent(sz -> hbuilder.setBlockSize(sz));
			HeaderProto header = hbuilder.build();
			supplier.onNext(CopyToHdfsFileRequest.newBuilder().setHeader(header).build());

			long totalUploaded = 0;
			while ( blocks.hasNext() ) {
				if ( nblocks % 4 == 0 ) {
					synchronized ( blocks ) {
						while ( !supplier.isReady() ) {
							blocks.wait(100);
						}
					}
				}
				
				ByteString block = ByteString.copyFrom(blocks.next());
				CopyToHdfsFileRequest req = CopyToHdfsFileRequest.newBuilder()
																.setBlock(block)
																.build();
				supplier.onNext(req);
				
				++nblocks;
				totalUploaded += block.size();
				if ( totalUploaded % REPORT_INTERVAL == 0 ) {
					logUploadProgress(path, totalUploaded, watch);
				}
			}
			supplier.onCompleted();
			logUploadProgress(path, totalUploaded, watch);
		}
		catch ( Throwable e ) {
			supplier.onError(e);
			
			Throwables.throwIfInstanceOf(e, IOException.class);
			throw Throwables.toRuntimeException(e);
		}
		
		try {
			value.get();
		}
		catch ( Throwable e ) {
			Throwables.throwIfInstanceOf(e, IOException.class);
			throw Throwables.toRuntimeException(e);
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
