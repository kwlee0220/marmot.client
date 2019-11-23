package marmot.remote.protobuf;

import java.io.IOException;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.TextFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import marmot.MarmotSparkSession;
import marmot.StoreDataSetOptions;
import marmot.exec.MarmotExecutionException;
import marmot.proto.service.MarmotSparkSessionServiceGrpc;
import marmot.proto.service.MarmotSparkSessionServiceGrpc.MarmotSparkSessionServiceBlockingStub;
import marmot.proto.service.RunSQLRequest;
import marmot.proto.service.ViewMappingProto;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotSparkSessionClient implements MarmotSparkSession {
	private final Server m_server;
	
	private final ManagedChannel m_channel;
	private final MarmotSparkSessionServiceBlockingStub m_blockingStub;
	
	public static PBMarmotSparkSessionClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new PBMarmotSparkSessionClient(channel);
	}
	
	private PBMarmotSparkSessionClient(ManagedChannel channel) throws IOException {
		m_channel = channel;

		m_blockingStub = MarmotSparkSessionServiceGrpc.newBlockingStub(channel);

		m_server = ServerBuilder.forPort(0).build();
		m_server.start();
	}
	
	public Server getGrpcServer() {
		return m_server;
	}
	
	public void close() {
		m_channel.shutdown();
		m_server.shutdown();
	}
	
	ManagedChannel getChannel() {
		return m_channel;
	}

	@Override
	public void createOrReplaceView(String viewName, String dsId) {
		ViewMappingProto mapping = ViewMappingProto.newBuilder()
													.setViewName(viewName)
													.setDsId(dsId)
													.build();
		PBUtils.handle(m_blockingStub.createOrReplaceView(mapping));
	}

	@Override
	public void runSql(String sqlStmt, String outDsId, StoreDataSetOptions opts)
		throws MarmotExecutionException {
		RunSQLRequest req = RunSQLRequest.newBuilder()
										.setSqlStatement(sqlStmt)
										.setOutputDsId(outDsId)
										.setOptions(opts.toProto())
										.build();
		PBUtils.handle(m_blockingStub.runSql(req));
	}
}
