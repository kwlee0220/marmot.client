package marmot.command;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParseResult;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_analysis",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="marmot analytics commands",
		subcommands = {
			MarmotAnalysisCommand.List.class,
			MarmotAnalysisCommand.Add.class,
			MarmotAnalysisCommand.Delete.class,
			MarmotAnalysisCommand.Show.class,
			MarmotAnalysisCommand.Run.class,
			MarmotAnalysisCommand.Cancel.class,
		})
public class RemoteMarmotAnalysisMain {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;

	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteMarmotAnalysisMain cmd = new RemoteMarmotAnalysisMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				ParseResult parsed = commandLine.parseArgs(args);
				ParseResult sub;
				while ( (sub = parsed.subcommand()) != null ) {
					parsed = sub;
				}
				CheckedConsumer<MarmotRuntime> handle = (CheckedConsumer<MarmotRuntime>)
														parsed.commandSpec().userObject();
				
				// 원격 MarmotServer에 접속.
				PBMarmotClient marmot = cmd.m_connector.connect();
				handle.accept(marmot);
			}
		}
		catch ( Throwable e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
