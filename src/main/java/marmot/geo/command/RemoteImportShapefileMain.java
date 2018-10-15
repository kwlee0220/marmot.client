package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ImportShapefile;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteImportShapefileMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_import_shapefile ");
		parser.addArgumentName("shp_file");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("dataset", "name", "dataset name", true);
		parser.addArgOption("srid", "code", "EPSG code for output dataset", true);
		parser.addArgOption("shp_srid", "code", "EPSG code for input shapefile", false);
		parser.addArgOption("charset", "charset", "Character encoding", true);
		parser.addArgOption("report_interval", "count", "progress report interval", false);
		parser.addOption("f", "force to create a new dataset", false);
		parser.addOption("a", "append to the existing dataset", false);
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
			ImportShapefile.run(marmot, cl);
			marmot.disconnect();
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
