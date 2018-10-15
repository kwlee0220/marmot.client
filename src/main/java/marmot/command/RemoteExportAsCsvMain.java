package marmot.command;

import marmot.externio.csv.ExportAsCsv;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteExportAsCsvMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);

		CommandLineParser parser = new CommandLineParser("mc_export_csv ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: $MARMOT_HOST)", false);
		parser.addArgOption("port", "number", "marmot server port (default: $MARMOT_PORT)", false);
		parser.addArgOption("output", "path", "output CSV file path", false);
		parser.addArgOption("delim", "char", "CSV delimiter character", false);
		parser.addArgOption("charset", "code", "character set code (default: utf-8)", false);
		parser.addArgOption("quote", "char", "quote character", false);
		parser.addArgOption("point_col", "col_names", "X,Y fields for point", false);
		parser.addArgOption("csv_srid", "code", "EPSG code for target CSV file", false);
		parser.addOption("header_first", "print field names first", false);
		parser.addOption("h", "print usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			long count = ExportAsCsv.run(marmot, cl);
			System.out.printf("done: %d records%n", count);
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
