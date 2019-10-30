package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import marmot.BindDataSetOptions;
import marmot.CreateDataSetOptions;
import marmot.DataSet;
import marmot.DataSetExistsException;
import marmot.DataSetType;
import marmot.ExecutePlanOptions;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.StoreDataSetOptions;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ExecutionNotFoundException;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecutionException;
import marmot.io.MarmotFileNotFoundException;
import marmot.proto.optor.OperatorProto;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBMarmotClient implements MarmotRuntime {
	private final Server m_server;
	
	private final ManagedChannel m_channel;
	private final PBFileServiceProxy m_fileService;
	private final PBDataSetServiceProxy m_dsService;
	private final PBPlanExecutionServiceProxy m_pexecService;
	private final boolean m_useCompression;
	
	public static PBMarmotClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new PBMarmotClient(channel, true);
	}
	
	private PBMarmotClient(ManagedChannel channel, boolean useCompression) throws IOException {
		m_channel = channel;
		m_useCompression = useCompression;
		
		m_fileService = new PBFileServiceProxy(this, channel);
		m_dsService = new PBDataSetServiceProxy(this, channel);
		m_pexecService = new PBPlanExecutionServiceProxy(this, channel);

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
	
	public boolean useCompression() {
		return m_useCompression;
	}
	
	public PBPlanExecutionServiceProxy getPlanExecutionService() {
		return m_pexecService;
	}

	@Override
	public RecordSet readMarmotFile(String path) throws MarmotFileNotFoundException {
		return m_fileService.readMarmotFile(path);
	}

	@Override
	public long copyToHdfsFile(String path, InputStream stream, FOption<Long> blockSize,
								FOption<String> codecName) throws IOException {
		return m_fileService.copyToHdfsFile(path, stream, blockSize, codecName);
	}

	@Override
	public void deleteHdfsFile(String path) throws IOException {
		m_fileService.deleteHdfsFile(path);
	}

	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	//	DataSet relateds
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

	@Override
	public DataSet createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
		throws DataSetExistsException {
		return m_dsService.createDataSet(dsId, schema, opts);
	}
	
	public DataSet createDataSet(String dsId, Plan plan, StoreDataSetOptions opts)
		throws DataSetExistsException {
		plan = adjustPlanForStore(plan, dsId, opts);
		execute(plan);
		return getDataSet(dsId);
	}
	
	private Plan adjustPlanForStore(Plan plan, String dsId, StoreDataSetOptions opts) {
		FOption<OperatorProto> last = plan.getLastOperator();
		if ( last.isAbsent() ) {
			throw new IllegalArgumentException("Plan is empty");
		}
		
		PlanBuilder builder;
		switch ( last.get().getOperatorCase() ) {
			case STORE_DATASET:
				List<OperatorProto> optors = plan.toProto().getOperatorsList();
				optors.remove(optors.size()-1);
				builder = FStream.from(optors)
								.foldLeft(planBuilder(plan.getName()), (b,o) -> b.add(o));
				break;
			default:
				builder = plan.toBuilder();
		}
		
		return builder.store(dsId, opts).build();
	}
	
	@Override
	public DataSet getDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		
		return m_dsService.getDataSet(dsId);
	}

	@Override
	public DataSet getDataSetOrNull(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		
		return m_dsService.getDataSetOrNull(dsId);
	}

	@Override
	public List<DataSet> getDataSetAll() {
		return m_dsService.getDataSetAll();
	}

	@Override
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		Utilities.checkNotNullArgument(folder, "dataset folder is null");
		
		return m_dsService.getDataSetAllInDir(folder, recursive);
	}

	@Override
	public void createKafkaTopic(String topic, boolean force) {
		m_dsService.createKafkaTopic(topic, force);
	}

	@Override
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										BindDataSetOptions opts) {
		return m_dsService.bindExternalDataSet(dsId, srcPath, type, opts);
	}

	@Override
	public boolean deleteDataSet(String id) {
		return m_dsService.deleteDataSet(id);
	}

	@Override
	public void moveDataSet(String id, String newId) {
		m_dsService.moveDataSet(id, newId);
	}

	@Override
	public List<String> getDirAll() {
		return m_dsService.getDirAll();
	}

	@Override
	public List<String> getSubDirAll(String folder, boolean recursive) {
		return m_dsService.getSubDirAll(folder, recursive);
	}

	@Override
	public String getParentDir(String folder) {
		return m_dsService.getParentDir(folder);
	}

	@Override
	public void moveDir(String path, String newPath) {
		m_dsService.renameDir(path, newPath);
	}

	@Override
	public void deleteDir(String folder) {
		m_dsService.deleteDir(folder);
	}

	/////////////////////////////////////////////////////////////////////
	// Plan Execution Relateds
	/////////////////////////////////////////////////////////////////////

	@Override
	public PlanBuilder planBuilder(String planName) {
		return new PlanBuilder(planName);
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan) {
		return m_pexecService.getOutputRecordSchema(plan, FOption.empty());
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan, RecordSchema inputSchema) {
		return m_pexecService.getOutputRecordSchema(plan, FOption.of(inputSchema));
	}

	@Override
	public MarmotAnalysis getAnalysis(String id) {
		return m_pexecService.getAnalysis(id);
	}

	@Override
	public MarmotAnalysis findAnalysis(String id) {
		return m_pexecService.findAnalysis(id);
	}

	@Override
	public CompositeAnalysis findParentAnalysis(String id) {
		return m_pexecService.findParentAnalysis(id);
	}

	@Override
	public List<CompositeAnalysis> getAncestorAnalysisAll(String id) {
		return m_pexecService.getAncestorAnalysisAll(id);
	}

	@Override
	public List<MarmotAnalysis> getDescendantAnalysisAll(String id) {
		return m_pexecService.getDescendantAnalysisAll(id);
	}

	@Override
	public List<MarmotAnalysis> getAnalysisAll() {
		return m_pexecService.getAnalysisAll();
	}

	@Override
	public void addAnalysis(MarmotAnalysis analysis, boolean force) {
		m_pexecService.addAnalysis(analysis, force);
	}

	@Override
	public void deleteAnalysis(String id, boolean recursive) {
		m_pexecService.deleteAnalysis(id, recursive);
	}

	@Override
	public void deleteAnalysisAll() {
		m_pexecService.deleteAnalysisAll();
	}

	@Override
	public MarmotExecution startAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException {
		return m_pexecService.startAnalysis(analysis);
	}

	@Override
	public void executeAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException {
		m_pexecService.executeAnalysis(analysis);
	}

	@Override
	public MarmotExecution getMarmotExecution(String id) throws ExecutionNotFoundException {
		return m_pexecService.getMarmotExecution(id);
	}

	@Override
	public List<MarmotExecution> getMarmotExecutionAll() {
		return m_pexecService.getMarmotExecutionAll();
	}
	
	@Override
	public void execute(Plan plan, ExecutePlanOptions opts) {
		m_pexecService.execute(plan, opts);
	}

	@Override
	public RecordSet executeLocally(Plan plan) {
		return m_pexecService.executeLocally(plan);
	}

	@Override
	public RecordSet executeLocally(Plan plan, RecordSet input) {
		return m_pexecService.executeLocally(plan, input);
	}

	@Override
	public FOption<Record> executeToRecord(Plan plan, ExecutePlanOptions opts) {
		return m_pexecService.executeToRecord(plan, opts);
	}

	@Override
	public RecordSet executeToRecordSet(Plan plan, ExecutePlanOptions opts) {
		return m_pexecService.executeToRecordSet(plan);
	}

	@Override
	public RecordSet executeToStream(String id, Plan plan) {
		return m_pexecService.executeToStream(id, plan);
	}

	@Override
	public RecordSchema getProcessOutputRecordSchema(String processId,
												Map<String, String> params) {
		return m_pexecService.getProcessRecordSchema(processId, params);
	}

	@Override
	public void executeProcess(String processId, Map<String, String> params) {
		m_pexecService.executeProcess(processId, params);
	}
	
	public void ping() {
		m_pexecService.ping();
	}
}
