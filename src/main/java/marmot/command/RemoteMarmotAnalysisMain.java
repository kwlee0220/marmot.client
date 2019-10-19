package marmot.command;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_analysis",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="marmot analytics commands",
		subcommands = {
			MarmotAnalysisCommands.ListAnalysis.class,
			MarmotAnalysisCommands.Add.class,
			MarmotAnalysisCommands.Delete.class,
			MarmotAnalysisCommands.Show.class,
			MarmotAnalysisCommands.Run.class,
		})
public class RemoteMarmotAnalysisMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteMarmotAnalysisMain cmd = new RemoteMarmotAnalysisMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
}
