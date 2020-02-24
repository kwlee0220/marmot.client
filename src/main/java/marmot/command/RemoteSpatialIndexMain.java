package marmot.command;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.RemoteSpatialIndexMain.CreateSpatialIndex;
import marmot.command.RemoteSpatialIndexMain.DeleteSpatialIndex;
import marmot.command.RemoteSpatialIndexMain.DrawSpatialIndex;
import marmot.command.RemoteSpatialIndexMain.ShowSpatialIndex;
import marmot.dataset.DataSet;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import marmot.externio.shp.ShapefileParameters;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.CreateSpatialIndexOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_spindex",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="spatial clustered index commands",
		subcommands = {
			CreateSpatialIndex.class,
			DeleteSpatialIndex.class,
			ShowSpatialIndex.class,
			DrawSpatialIndex.class,
		})
public class RemoteSpatialIndexMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteSpatialIndexMain cmd = new RemoteSpatialIndexMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}

	@Command(name="create", description="cluster the dataset")
	static class CreateSpatialIndex extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Option(names="-sample_ratio", paramLabel="ratio", description="sampling ratio (0:1]")
		private double m_sampleRatio;

		@Option(names= {"-c", "-cluster_size"}, paramLabel="nbytes", description="cluster size (eg: '64mb')")
		private void setClusterSize(String sizeStr) {
			m_clusterSize = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterSize = -1;

		@Option(names= {"-b", "-block_size"}, paramLabel="nbytes", description="block size (eg: '64mb')")
		private void setBlockSize(String blkSizeStr) {
			m_blkSize = UnitUtils.parseByteSize(blkSizeStr);
		}
		private long m_blkSize = -1;
		
		@Option(names="-workers", paramLabel="count", description="reduce task count")
		private int m_nworkers = -1;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			CreateSpatialIndexOptions options = CreateSpatialIndexOptions.DEFAULT();
			if ( m_clusterSize > 0 ) {
				options = options.clusterSize(m_clusterSize);
			}
			if ( m_sampleRatio > 0 ) {
				options = options.sampleRatio(m_sampleRatio);
			}
			if ( m_blkSize > 0 ) {
				options = options.blockSize(m_blkSize);
			}
			if ( m_nworkers > 0 ) {
				options = options.workerCount(m_nworkers);
			}
			
			DataSet ds = marmot.getDataSet(m_dsId);
			SpatialIndexInfo idxInfo = ds.cluster(options);
			
			System.out.printf("clustered: nclusters=%d nrecords=%d, non-duplicated=%d%n",
							idxInfo.getClusterCount(), idxInfo.getRecordCount(),
							idxInfo.getNonDuplicatedRecordCount());
		}
	}

	@Command(name="delete", description="delete the cluster of the dataset")
	static class DeleteSpatialIndex extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to cluster"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			ds.deleteSpatialCluster();
		}
	}

	@Command(name="show", description="display spatial cluster information for a dataset")
	static class ShowSpatialIndex extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to cluster"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			Plan plan;
			plan = Plan.builder("list_spatial_clusters")
						.loadSpatialClusterIndexFile(m_dsId)
						.project("*-{bounds,value_envelope}")
						.build();
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				rset.forEach(r -> printIndexEntry(r));
			}
		}
		
		private static final void printIndexEntry(Record record) {
			String packId = record.getString("pack_id");
			int blockNo = record.getInt("block_no");
			String quadKey = record.getString("quad_key");
			long count = record.getLong("count");
			long ownedCount = record.getLong("owned_count");
			String start = UnitUtils.toByteSizeString(record.getLong("start"));
			String len = UnitUtils.toByteSizeString(record.getLong("length"));
			
			System.out.printf("pack_id=%s, block_no=%02d, quad_key=%s, count=%d(%d), start=%s, length=%s%n",
								packId, blockNo, quadKey, count, ownedCount, start, len);
		}
	}

	@Command(name="draw", description="create a shapefile for spatial cluster tiles of a dataset")
	static class DrawSpatialIndex extends SubCommand {
		@Mixin private ShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1", description={"id of the target dataset"})
		private String m_dsId;
		
		@Option(names={"-o", "-output_dir"}, paramLabel="output-directory", required=true,
				description={"directory path for the output"})
		private String m_output;
		
		@Option(names={"-f"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-value"}, description="draw value envelope")
		private boolean m_drawValue;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			String toGeom = (m_drawValue) ? "ST_GeomFromEnvelope(data_bounds)"
											: "ST_GeomFromEnvelope(tile_bounds)";
			
			Plan plan = Plan.builder("read_cluster_index")
								.loadSpatialClusterIndexFile(m_dsId)
								.defineColumn("the_geom:polygon", toGeom)
								.project("the_geom,pack_id,quad_key,count,length")
								.build();
			
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				ExportShapefileParameters params = ExportShapefileParameters.create()
															.charset(m_shpParams.charset());
				ExportRecordSetAsShapefile exporter = new ExportRecordSetAsShapefile(rset, "EPSG:4326",
																					m_output, params);
				exporter.setForce(m_force);
				exporter.start().waitForDone();
			}
		}
	}
}
