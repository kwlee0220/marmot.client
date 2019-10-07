package marmot.command;

import marmot.command.plan.BuildPlanCommand;
import marmot.command.plan.PlanCommands;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_plan",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="plan-related commands",
		subcommands = {
			BuildPlanCommand.class,
			PlanCommands.Show.class,
			PlanCommands.Schema.class,
			PlanCommands.Run.class,
			PlanCommands.Start.class,
		})
public class RemotePlanMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemotePlanMain cmd = new RemotePlanMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
}
