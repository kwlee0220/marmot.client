package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.externio.geojson.ImportGeoJson;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteImportGeoJsonMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_import_geojson ");
		parser.addArgumentName("geojson_file");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("charset", "charset", "Character encoding (default: utf-8)", false);
		parser.addArgOption("src_srid", "code", "EPSG code for input GeoJSON file", false);
		parser.addArgOption("dataset", "name", "dataset name", true);
		parser.addArgOption("geom_col", "name", "Geometry column name for output dataset (default: the_geom)", false);
		parser.addArgOption("srid", "code", "EPSG code for output dataset", true);
		parser.addArgOption("block_size", "nbytes", "block size (eg: '64mb')", false);
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
			ImportGeoJson.runCommand(marmot, cl);
			marmot.disconnect();
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
