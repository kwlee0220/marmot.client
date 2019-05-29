package marmot.remote.protobuf;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import marmot.DataSet;
import marmot.DataSetExistsException;
import marmot.DataSetNotFoundException;
import marmot.DataSetType;
import marmot.ExecutePlanOptions;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.StoreDataSetOptions;
import utils.Utilities;
import utils.func.FOption;

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
	
	public static PBMarmotClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new PBMarmotClient(channel);
	}
	
	private PBMarmotClient(ManagedChannel channel) throws IOException {
		m_channel = channel;
		
		m_fileService = new PBFileServiceProxy(this, channel);
		m_dsService = new PBDataSetServiceProxy(this, channel);
		m_pexecService = new PBPlanExecutionServiceProxy(this, channel);

		m_server = ServerBuilder.forPort(0).build();
		m_server.start();
	}
	
	public Server getGrpcServer() {
		return m_server;
	}
	
	public void disconnect() {
		m_channel.shutdown();
		m_server.shutdown();
	}
	
	ManagedChannel getChannel() {
		return m_channel;
	}
	
	public PBPlanExecutionServiceProxy getPlanExecutionService() {
		return m_pexecService;
	}

	@Override
	public void copyToHdfsFile(String path, Iterator<byte[]> blocks, FOption<Long> blockSize)
		throws IOException {
		m_fileService.copyToHdfsFile(path, blocks, blockSize);
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
	public DataSet createDataSet(String dsId, RecordSchema schema, StoreDataSetOptions opts)
		throws DataSetExistsException {
		return m_dsService.createDataSet(dsId, schema, opts);
	}
	
	@Override
	public DataSet createDataSet(String dsId, Plan plan, ExecutePlanOptions execOpts,
									StoreDataSetOptions opts)
		throws DataSetExistsException {
		return m_dsService.createDataSet(dsId, plan, execOpts, opts);
	}
	
	@Override
	public DataSet appendIntoDataSet(String dsId, Plan plan, ExecutePlanOptions execOpts)
		throws DataSetNotFoundException {
		return m_dsService.appendIntoDataSet(dsId, plan, execOpts);
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
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type) {
		return m_dsService.bindExternalDataSet(dsId, srcPath, type, FOption.empty());
	}

	@Override
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										GeometryColumnInfo geomColInfo) {
		return m_dsService.bindExternalDataSet(dsId, srcPath, type, FOption.of(geomColInfo));
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

	@Override
	public void executeModule(String id) {
		m_pexecService.executeModule(id);
	}
}
