package marmot.command;

import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_run_module",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="run module")
public class RemoteRunModuleMain extends RunModuleCommand {
	@Mixin protected MarmotConnector m_connector;
	
	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteRunModuleMain cmd = new RemoteRunModuleMain();
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
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
