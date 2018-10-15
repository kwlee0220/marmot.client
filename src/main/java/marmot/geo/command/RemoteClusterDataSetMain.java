package marmot.geo.command;

import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteClusterDataSetMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_cluster_dataset ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("sample_ratio", "ratio", "sampling ratio (0:1]", false);
		parser.addArgOption("fill_ratio", "ratio", "max block-fill ratio", false);
		parser.addArgOption("block_size", "nbytes", "cluster size (eg: '64mb')", false);
		parser.addArgOption("workers", "count", "reduce task count", false);
		parser.addOption("h", "show usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			String dsId = cl.getArgument("dataset");
			
			ClusterDataSetOptions options = new ClusterDataSetOptions();
			options.quadKeyFilePath(cl.getOptionString("qkeys"));
			options.blockFillRatio(cl.getOptionDouble("fill_ratio"));
			options.sampleRatio(cl.getOptionDouble("sample_ratio"));
			options.blockSize(cl.getOptionString("block_size")
								.map(UnitUtils::parseByteSize));
			options.workerCount(cl.getOptionInt("workers"));
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			DataSet ds = marmot.getDataSet(dsId);
			ds.cluster();
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
