package marmot.geo.command;

import static utils.UnitUtils.parseByteSize;

import com.vividsolutions.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.analysis.module.geo.ClusterSpatialDataSetParameters;
import marmot.command.MarmotClientCommand;
import marmot.command.MarmotClientCommands;
import marmot.command.PicocliCommands;
import marmot.command.PicocliCommands.SubCommand;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import marmot.externio.shp.ShapefileParameters;
import marmot.geo.command.RemoteSpatialClusterMain.CreateSpatialCluster;
import marmot.geo.command.RemoteSpatialClusterMain.DrawSpatialClusterInfo;
import marmot.geo.command.RemoteSpatialClusterMain.ShowSpatialClusterInfos;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.func.Funcs;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_spcluster",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="spatial cluster commands",
		subcommands = {
			CreateSpatialCluster.class,
			ShowSpatialClusterInfos.class,
			DrawSpatialClusterInfo.class,
		})
public class RemoteSpatialClusterMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteSpatialClusterMain cmd = new RemoteSpatialClusterMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}

	@Command(name="create", description="cluster the dataset")
	static class CreateSpatialCluster extends SubCommand {
		@Parameters(paramLabel="input-id", index="0", arity="1..1", description={"input dataset id"})
		private String m_inputDsId;
		@Parameters(paramLabel="output-id", index="1", arity="1..1", description={"output dataset id"})
		private String m_outputDsId;
		
		@Option(names="-quadkey_dataset", paramLabel="dataset-id", description="quadkey dataset id")
		private String m_quadKeyDsId = null;
		
		@Option(names="-quadkey_length", paramLabel="length", description="maximum quad-key length")
		private int m_quadKeyLength = -1;
		
		@Option(names="-sample_size", paramLabel="nbytes", description="total sampling data size (eg: '128mb')")
		private void setSampleSize(String sizeStr) {
			m_sampleSize = parseByteSize(sizeStr);
		}
		private long m_sampleSize = -1;

		@Option(names= {"-cluster_size"}, paramLabel="nbytes", required=true,
				description="cluster size (eg: '64mb')")
		private void setClusterSize(String sizeStr) {
			m_clusterSize = parseByteSize(sizeStr);
		}
		private long m_clusterSize = -1;

		@Option(names={"-c", "-compress"}, paramLabel="codec", description="compression codec name")
		private String m_compressCodec = null;

		@Option(names= {"-b", "-block_size"}, paramLabel="nbytes", description="block size (eg: '64mb')")
		private void setBlockSize(String sizeStr) {
			m_blkSize = parseByteSize(sizeStr);
		}
		private long m_blkSize = -1;

		@Option(names={"-f", "-force"}, description="force to create a new dataset")
		private boolean m_force = false;
		
		@Option(names="-workers", paramLabel="count", description="cluster writer count")
		private int m_workerCount = -1;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			ClusterSpatialDataSetParameters params = new ClusterSpatialDataSetParameters();
			params.inputDataSet(m_inputDsId);
			params.outputDataSet(m_outputDsId);
			
			if ( m_quadKeyDsId != null ) {
				params.quadKeyDataSet(m_quadKeyDsId);
			}
			else {
				if ( m_sampleSize < 0 ) {
					throw new IllegalArgumentException("sample-size is missing");
				}
				params.sampleSize(m_sampleSize);

				if ( m_quadKeyLength > 0 ) {
					params.maxQuadKeyLength(m_quadKeyLength);
				}
			}
			
			params.clusterSize(m_clusterSize);
			if ( m_workerCount > 0 ) {
				params.workerCount(m_workerCount);
			}
			params.force(m_force);
			if ( m_blkSize > 0 ) {
				params.blockSize(m_blkSize);
			}
			Funcs.acceptIfNotNull(m_compressCodec, params::compressionCodecName);
			
			marmot.executeProcess(ClusterSpatialDataSetParameters.moduleName(), params.toMap());
		}
	}

	@Command(name="show", description="display spatial cluster information for a dataset")
	static class ShowSpatialClusterInfos extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to cluster"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			if ( ds.getType() != DataSetType.SPATIAL_CLUSTER ) {
				throw new IllegalArgumentException("dataset is not spatially clustered: ds=" + m_dsId);
			}
			
			String infoPath = ds.getHdfsPath() + "/cluster.idx";
			
			Plan plan;
			plan = Plan.builder("list_spatial_clusters")
						.loadMarmotFile(infoPath)
						.build();
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				rset.forEach(r -> printSpatialClusterInfo(r));
			}
		}
		
		private static final void printSpatialClusterInfo(Record record) {
			String quadKey = record.getString("quad_key");
			long count = record.getLong("count");
			Envelope bounds = (Envelope)record.get("data_bounds");
			
			System.out.printf("quad_key=%s, count=%d, bounds=%s%n", quadKey, count, bounds);
		}
	}

	@Command(name="draw", description="create a shapefile for spatial cluster infos of a dataset")
	static class DrawSpatialClusterInfo extends SubCommand {
		@Mixin private ShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1",
					description={"id of the target dataset"})
		private String m_dsId;
		
		@Option(names={"-o", "-output_dir"}, paramLabel="output-directory", required=true,
				description={"directory path for the output"})
		private String m_output;
		
		@Option(names={"-f"}, description="force to create a new output directory")
		private boolean m_force;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			String srid = ds.getGeometryColumnInfo().srid();
			String infoPath = ds.getHdfsPath() + "/cluster.idx";
			
			Plan plan = Plan.builder("read_cluster_index")
							.loadMarmotFile(infoPath)
							.defineColumn("the_geom:polygon",  "ST_GeomFromEnvelope(data_bounds)")
							.project("the_geom,quadKey,count")
							.build();
			
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				ExportShapefileParameters params = ExportShapefileParameters.create()
															.charset(m_shpParams.charset());
				ExportRecordSetAsShapefile exporter = new ExportRecordSetAsShapefile(rset, srid,
																					m_output, params);
				exporter.setForce(m_force);
				exporter.start().waitForDone();
			}
		}
	}
}
