package marmot.command;

import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CountRecordsMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_count ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("key", "key columns", "group key columns", false);
		parser.addOption("h", "help usage");
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("h") ) {
			cl.exitWithUsage(0);
		}

		try {
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			String dsId = cl.getArgument("dataset");
			String keyCols = cl.getOptionString("key").getOrNull();
			
			PlanBuilder builder = marmot.planBuilder("count records")
										.load(dsId);
			if ( keyCols != null ) {
				builder = builder.distinct(keyCols);
			}
			Plan plan = builder.aggregate(AggregateFunction.COUNT())
								.build();
			try ( RecordSet result = marmot.executeToRecordSet(plan) ) {
				long count = result.fstream().mapToLong(r -> r.getLong(0)).next().get();
				System.out.println(count);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
}
