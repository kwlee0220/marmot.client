package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.process.NormalizeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NormalizeProcessMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_process_normalize ");
		parser.addArgumentName("input_dataset");
		parser.addArgumentName("output_dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("in_features", "cols", "input feature columns", true);
		parser.addArgOption("out_features", "cols", "output feature columns", true);
		parser.addOption("f", "force to create a new dataset", false);
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
			
			String inDsId = cl.getArgument("input_dataset");
			String outDsId = cl.getArgument("output_dataset");
			String[] inFeatures = CSV.parseCSVAsArray(cl.getString("in_features"));
			String[] outFeatures = CSV.parseCSVAsArray(cl.getString("out_features"));
			
			NormalizeParameters params = new NormalizeParameters();
			params.inputDataset(inDsId);
			params.outputDataset(outDsId);
			params.inputFeatureColumns(inFeatures);
			params.outputFeatureColumns(outFeatures);
			marmot.executeProcess("normalize", params.toMap());
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
