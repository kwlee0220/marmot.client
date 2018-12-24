package marmot.command;

import marmot.geo.command.BindDataSetCommand;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_bind_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="bind the existing file(s) as a dataset")
public class RemoteBindDataSetMain extends BindDataSetCommand {
	@Mixin private MarmotConnector m_connector;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteBindDataSetMain cmd = new RemoteBindDataSetMain();
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
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
