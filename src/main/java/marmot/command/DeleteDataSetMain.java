package marmot.command;

import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DeleteDataSetMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_delete ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: $MARMOT_HOST)", false);
		parser.addArgOption("port", "number", "marmot server port (default: $MARMOT_PORT)", false);
		parser.addOption("r", "delete all recursively");
		parser.addOption("h", "print usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			String dsId = cl.getArgument("dataset");
			boolean recursively = cl.hasOption("r");
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			if ( recursively ) {
				marmot.deleteDir(dsId);
			}
			else {
				marmot.deleteDataSet(dsId);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
