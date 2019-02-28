package marmot.command;

import marmot.Plan;
import marmot.RecordSet;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_count", description="count the total number of records",
			mixinStandardHelpOptions=false)
public class RemoteCountRecordsMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="dataset_id", description={"dataset id to count"})
	private String m_dsId;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteCountRecordsMain cmd = new RemoteCountRecordsMain();
		CommandLine commandLine = new CommandLine(cmd);
		
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void run() {
		try {
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = m_connector.connect();
			
			Plan plan = marmot.planBuilder("count records")
								.load(m_dsId)
								.aggregate(AggregateFunction.COUNT())
								.build();
			try ( RecordSet result = marmot.executeToRecordSet(plan) ) {
				long count = result.fstream().mapToLong(r -> r.getLong(0)).next().get();
				System.out.println(count);
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
}
