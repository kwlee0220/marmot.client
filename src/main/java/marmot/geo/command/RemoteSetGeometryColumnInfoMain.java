package marmot.geo.command;

import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.command.UsageHelp;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_set_gcinfo",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="set geometry column info")
public class RemoteSetGeometryColumnInfoMain extends SetGeometryColumnInfoCommand {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteSetGeometryColumnInfoMain cmd = new RemoteSetGeometryColumnInfoMain();
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
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
