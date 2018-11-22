package marmot.remote.protobuf;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.DataSetExistsException;
import marmot.DataSetOption;
import marmot.DataSetType;
import marmot.ExecutePlanOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

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
													.usePlaintext(true)
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
	public void copyToHdfsFile(String path, Iterator<byte[]> blocks, Option<Long> blockSize)
		throws IOException {
		m_fileService.copyToHdfsFile(path, blocks, blockSize);
	}

	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	//	DataSet relateds
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

	@Override
	public DataSet createDataSet(String dsId, RecordSchema schema, DataSetOption... opts)
		throws DataSetExistsException {
		return m_dsService.createDataSet(dsId, schema, opts);
	}
	
	@Override
	public DataSet createDataSet(String dsId, Plan plan, ExecutePlanOption[] execOpts,
									DataSetOption... opts)
		throws DataSetExistsException {
		return m_dsService.createDataSet(dsId, plan, execOpts, opts);
	}
	
	@Override
	public DataSet getDataSet(String dsId) {
		Objects.requireNonNull(dsId, "dataset id is null");
		
		return m_dsService.getDataSet(dsId);
	}

	@Override
	public DataSet getDataSetOrNull(String dsId) {
		Objects.requireNonNull(dsId, "dataset id is null");
		
		return m_dsService.getDataSetOrNull(dsId);
	}

	@Override
	public List<DataSet> getDataSetAll() {
		return m_dsService.getDataSetAll();
	}

	@Override
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		Objects.requireNonNull(folder, "dataset folder is null");
		
		return m_dsService.getDataSetAllInDir(folder, recursive);
	}

	@Override
	public void createKafkaTopic(String topic, boolean force) {
		m_dsService.createKafkaTopic(topic, force);
	}

	@Override
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type) {
		return m_dsService.bindExternalDataSet(dsId, srcPath, type, Option.none());
	}

	@Override
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										GeometryColumnInfo geomColInfo) {
		return m_dsService.bindExternalDataSet(dsId, srcPath, type, Option.some(geomColInfo));
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
	public boolean getMapOutputCompression() {
		return m_pexecService.getMapOutputCompression();
	}

	@Override
	public boolean setMapOutputCompression(boolean flag) {
		return m_pexecService.setMapOutputCompression(flag);
	}

	@Override
	public PlanBuilder planBuilder(String planName) {
		return new PlanBuilder(planName);
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan) {
		return m_pexecService.getOutputRecordSchema(plan, Option.none());
	}

	@Override
	public RecordSchema getOutputRecordSchema(Plan plan, RecordSchema inputSchema) {
		return m_pexecService.getOutputRecordSchema(plan, Option.some(inputSchema));
	}
	
	@Override
	public void execute(Plan plan, ExecutePlanOption... opts) {
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
	public Option<Record> executeToRecord(Plan plan, ExecutePlanOption... opts) {
		return m_pexecService.executeToRecord(plan, opts);
	}

	@Override
	public RecordSet executeToRecordSet(Plan plan, ExecutePlanOption... opts) {
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
