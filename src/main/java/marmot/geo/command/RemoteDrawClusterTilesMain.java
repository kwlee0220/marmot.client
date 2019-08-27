package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.command.UsageHelp;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import marmot.externio.shp.ShapefileParameters;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_draw_cluster_tiles",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="create a shapefile for spatial cluster tiles of a dataset")
public class RemoteDrawClusterTilesMain implements CheckedConsumer<MarmotRuntime> {
	@Mixin private MarmotConnector m_connector;
	@Mixin private ShapefileParameters m_shpParams;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="dataset-id", index="0", arity="1..1", description={"id of the target dataset"})
	private String m_dsId;
	
	@Option(names={"-o", "-output_dir"}, paramLabel="output-directory", required=true,
			description={"directory path for the output"})
	private String m_output;
	
	@Option(names={"-f"}, description="force to create a new output directory")
	private boolean m_force;
	
	@Option(names={"-v", "-value"}, description="draw value envelope")
	private boolean m_drawValue;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteDrawClusterTilesMain cmd = new RemoteDrawClusterTilesMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				PBMarmotClient marmot = cmd.m_connector.connect();
				cmd.accept(marmot);
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		String toGeom = (m_drawValue) ? "ST_GeomFromEnvelope(data_bounds)"
										: "ST_GeomFromEnvelope(tile_bounds)";
		
		Plan plan = marmot.planBuilder("read_cluster_index")
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
