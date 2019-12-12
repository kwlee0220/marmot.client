package marmot.remote.protobuf;

import java.io.IOException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import marmot.MarmotSparkSession;
import marmot.exec.MarmotExecutionException;
import marmot.optor.StoreDataSetOptions;
import marmot.proto.service.MarmotSparkSessionServiceGrpc;
import marmot.proto.service.MarmotSparkSessionServiceGrpc.MarmotSparkSessionServiceBlockingStub;
import marmot.proto.service.RunSQLRequest;
import marmot.proto.service.ViewMappingProto;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotSparkSessionClient extends PBMarmotClient implements MarmotSparkSession {
	private final MarmotSparkSessionServiceBlockingStub m_sparkStub;
	
	public static PBMarmotSparkSessionClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new PBMarmotSparkSessionClient(channel);
	}
	
	protected PBMarmotSparkSessionClient(ManagedChannel channel) throws IOException {
		super(channel, true);

		m_sparkStub = MarmotSparkSessionServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public void createOrReplaceView(String viewName, String dsId) {
		ViewMappingProto mapping = ViewMappingProto.newBuilder()
													.setViewName(viewName)
													.setDsId(dsId)
													.build();
		PBUtils.handle(m_sparkStub.createOrReplaceView(mapping));
	}

	@Override
	public void runSql(String sqlStmt, String outDsId, StoreDataSetOptions opts)
		throws MarmotExecutionException {
		RunSQLRequest req = RunSQLRequest.newBuilder()
										.setSqlStatement(sqlStmt)
										.setOutputDsId(outDsId)
										.setOptions(opts.toProto())
										.build();
		PBUtils.handle(m_sparkStub.runSql(req));
	}
}
