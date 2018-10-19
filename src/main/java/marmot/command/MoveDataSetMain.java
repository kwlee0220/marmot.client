package marmot.command;

import marmot.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MoveDataSetMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_move ");
		parser.addArgumentName("src");
		parser.addArgumentName("tar");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("h", "show usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
	
			String src = cl.getArgument("src");
			String tar = cl.getArgument("tar");
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
	
			DataSet srcDs = marmot.getDataSet(src);
			marmot.moveDataSet(srcDs.getId(), tar);
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
