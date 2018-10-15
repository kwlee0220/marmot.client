package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ExportDataSetAsShapefile;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteExportAsShapefileMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_export_shapefile ");
		parser.addArgumentName("dataset");
		parser.addArgOption("output_dir", "path", "directory path where output shpfiles are put", true);
		parser.addArgOption("charset", "name", "character encoding (default: utf-8)", false);
		parser.addArgOption("split_size", "size", "max size (shp and dbf), (default: 2gb)", false);
		parser.addOption("f", "delete the output directory if it exists already", false);
		parser.addArgOption("report_interval", "count", "progress report interval", false);
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
			
			long count = ExportDataSetAsShapefile.run(marmot, cl);
			System.out.printf("done: %d records%n", count);
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
