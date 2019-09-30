package marmot.geo.command;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.command.MarmotClientCommands;
import marmot.command.PlanBasedMarmotCommand;
import marmot.plan.SpatialJoinOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_spatial_join",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="SpatialJoin-based analysis")
public class RemoteSpatialJoinMain extends PlanBasedMarmotCommand {
	@Mixin Params m_params;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteSpatialJoinMain cmd = new RemoteSpatialJoinMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run("spatial_join", cmd.m_params.m_outputDsId);
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder) {
		setJoinDisabled();

		SpatialJoinOptions opts = SpatialJoinOptions.EMPTY;
		
		String joinExpr = m_opParams.getJoinExpr() != null ? m_opParams.getJoinExpr() : "intersects";
		opts = opts.joinExpr(joinExpr);
		if ( m_opParams.getJoinOutputColumns() != null ) {
			opts = opts.outputColumns(m_opParams.getJoinOutputColumns());
		}
		
		return builder.loadSpatialIndexJoin(m_params.m_leftDsId, m_params.m_rightDsId, opts);
	}
	
	private static class Params {
		@Parameters(paramLabel="left_dataset", index="0", arity="1..1", description={"left dataset id"})
		private String m_leftDsId;
		
		@Parameters(paramLabel="right_dataset", index="1", arity="1..1", description={"right dataset id"})
		private String m_rightDsId;
		
		@Parameters(paramLabel="output_dataset", index="2", arity="1..1", description={"output dataset id"})
		private String m_outputDsId;
	}
}
