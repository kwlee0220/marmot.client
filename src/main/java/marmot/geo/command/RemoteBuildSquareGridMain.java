package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_build_square_grid",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="build a square-grid for a dataset")
public class RemoteBuildSquareGridMain extends BuildSquareGridCommand {
	@Mixin private MarmotConnector m_connector;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteBuildSquareGridMain cmd = new RemoteBuildSquareGridMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}

			PBMarmotClient marmot = cmd.m_connector.connect();
			cmd.accept(marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
