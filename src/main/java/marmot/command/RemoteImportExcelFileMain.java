package marmot.command;

import marmot.externio.excel.ImportExcel;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteImportExcelFileMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_import_excel ");
		parser.addArgumentName("excel_file");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("header_first", "get the first line as header", false);
		parser.addArgOption("point_col", "x:y", "X,Y fields for point", false);
		parser.addArgOption("excel_srid", "code", "EPSG code for the input Excel file", false);
		parser.addArgOption("null_string", "string", "null string", false);
		parser.addArgOption("dataset", "name", "dataset name", true);
		parser.addArgOption("geom_col", "name", "default Geometry column", false);
		parser.addArgOption("srid", "code", "EPSG code for result dataset", false);
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
			ImportExcel.runCommand(marmot, cl);
			marmot.disconnect();
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(0);
		}
	}
}
