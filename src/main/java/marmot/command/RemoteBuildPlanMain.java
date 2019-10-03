package marmot.command;

import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_build_plan",
				parameterListHeading = "Parameters:%n",
				optionListHeading = "Options:%n",
				description="build-up a plan")
public class RemoteBuildPlanMain extends BuildPlanCommand {
	@Mixin private MarmotConnector m_connector;
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteBuildPlanMain cmd = new RemoteBuildPlanMain();
		CommandLine commandLine = new CommandLine(cmd);
		
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				// 원격 MarmotServer에 접속.
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
