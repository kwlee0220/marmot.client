package marmot.geo.command;

import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ImportShapefile;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteSpatialJoinMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_import_shapefile ");
		parser.addArgumentName("left");
		parser.addArgumentName("right");
		parser.addArgumentName("output");
		parser.addOption("h", "help usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);

			String left = cl.getArgument("left");
			String right = cl.getArgument("right");
			String output = cl.getArgument("output");

			Plan plan = marmot.planBuilder("test_spatial_join")
								.loadSpatialIndexJoin(left, right)
								.store(output)
								.build();
			marmot.createDataSet(output, plan, DataSetOption.FORCE);
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
