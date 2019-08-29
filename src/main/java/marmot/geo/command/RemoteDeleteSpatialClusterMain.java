package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.command.UsageHelp;
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
@Command(name="mc_delete_spatial_cluster",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="delete spatial cluster")
public class RemoteDeleteSpatialClusterMain implements CheckedConsumer<MarmotRuntime> {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
				description={"dataset id to cluster"})
	private String m_dsId;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteDeleteSpatialClusterMain cmd = new RemoteDeleteSpatialClusterMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
				return;
			}

			PBMarmotClient marmot = cmd.m_connector.connect();
			cmd.accept(marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		DataSet ds = marmot.getDataSet(m_dsId);
		ds.deleteSpatialCluster();
	}
}
