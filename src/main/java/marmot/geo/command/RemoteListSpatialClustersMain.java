package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_list_spatial_clusters",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="display spatial cluster information for a dataset")
public class RemoteListSpatialClustersMain extends ListSpatialClustersCommand {
	@Mixin private MarmotConnector m_connector;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteListSpatialClustersMain cmd = new RemoteListSpatialClustersMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				PBMarmotClient marmot = cmd.m_connector.connect();
				cmd.accept(marmot);
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
