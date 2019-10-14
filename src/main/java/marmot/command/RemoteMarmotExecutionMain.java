package marmot.command;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_exec",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="marmot execution related commands",
		subcommands = {
			MarmotExecutionCommands.Show.class,
			MarmotExecutionCommands.ListExecs.class,
			MarmotExecutionCommands.Cancel.class,
		})
public class RemoteMarmotExecutionMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteMarmotExecutionMain cmd = new RemoteMarmotExecutionMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
}
