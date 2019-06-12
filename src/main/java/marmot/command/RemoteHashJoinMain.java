package marmot.command;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_hash_join", description="hash join", mixinStandardHelpOptions=false)
public class RemoteHashJoinMain extends PlanBasedMarmotCommand {
	@Mixin HashJoinParams m_params;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteHashJoinMain cmd = new RemoteHashJoinMain();
		CommandLine commandLine = new CommandLine(cmd);
		commandLine.parse(args);
		
		if ( commandLine.isUsageHelpRequested() ) {
			commandLine.usage(System.out, Ansi.OFF);
		}
		else {
			try {
				cmd.run("hash_join", cmd.m_params.m_outputDsId);
			}
			catch ( Exception e ) {
				System.err.printf("failed: %s%n%n", e);
				commandLine.usage(System.out, Ansi.OFF);
			}
		}
	}

	@Override
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder) {
		JoinType joinType = JoinType.fromString(m_params.m_joinType + "_join");
		JoinOptions opts = new JoinOptions().joinType(joinType);
		if ( m_params.m_nworkers > 0 ) {
			opts = opts.workerCount(m_params.m_nworkers);
		}
		
		return builder.loadHashJoin(m_params.m_leftDsId, m_params.m_leftCols,
									m_params.m_rightDsId, m_params.m_rightCols,
									m_params.m_outputCols, opts);
	}
	
	private static class HashJoinParams {
		@Parameters(paramLabel="left_dataset", index="0", description={"left dataset id"})
		private String m_leftDsId;
		
		@Parameters(paramLabel="left_join_cols", index="1", description={"left join columns"})
		private String m_leftCols;
		
		@Parameters(paramLabel="right_dataset", index="2", description={"right dataset id"})
		private String m_rightDsId;
		
		@Parameters(paramLabel="right_join_cols", index="3", description={"right join columns"})
		private String m_rightCols;
		
		@Parameters(paramLabel="output_dataset", index="4", description={"output dataset id"})
		private String m_outputDsId;
		
		@Parameters(paramLabel="output_cols", index="5", description={"output columns"})
		private String m_outputCols;
		
		@Parameters(paramLabel="join_type", index="6", arity="0..1", description={"hash join type"})
		private String m_joinType = "inner";
		
		@Parameters(paramLabel="nworkers", index="7", arity="0..1", description={"join workers"})
		private int m_nworkers = -1;
	}
}
