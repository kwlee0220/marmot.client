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
import utils.DelayedSplitter;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_hash_join",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="HashJoin-based analysis")
public class RemoteHashJoinMain extends PlanBasedMarmotCommand {
	@Mixin Params m_params;
	
	private static class Params {
		@Parameters(paramLabel="left_dataset_spec", index="0", arity="1..1",
					description={"left dataset id and join columns, eg.) 'ds_id:col1,col2'"})
		private String m_leftDsSpec;
		
		@Parameters(paramLabel="right_dataset_spec", index="1", arity="1..1",
					description={"right dataset id and join columns, eg.) 'ds_id:col1,col2'"})
		private String m_rightDsSpec;
		
		@Parameters(paramLabel="output_dataset", index="3", arity="1..1",
					description={"output dataset id"})
		private String m_outputDsId;
	}

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteHashJoinMain cmd = new RemoteHashJoinMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
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
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder) {
		setJoinDisabled();
		
		JoinType joinType = JoinType.fromString(m_opParams.m_joinType + "_join");
		JoinOptions opts = JoinOptions.create(joinType);
		if ( m_opParams.m_joinWorkers > 0 ) {
			opts = opts.workerCount(m_opParams.m_joinWorkers);
		}
		
		if ( m_opParams.m_joinOutCols == null ) {
			throw new IllegalArgumentException("'join_output_col' is not present");
		}
		
		DelayedSplitter splitter = DelayedSplitter.on(m_params.m_leftDsSpec);
		String leftDsId = splitter.cutNext(':')
								.getOrElseThrow(() -> new IllegalArgumentException("left dataset spec: " + m_params.m_leftDsSpec));
		String leftCols = splitter.remains()
								.getOrElseThrow(() -> new IllegalArgumentException("left dataset spec: " + m_params.m_leftDsSpec));
		splitter = DelayedSplitter.on(m_params.m_rightDsSpec);
		String rightDsId = splitter.cutNext(':')
								.getOrElseThrow(() -> new IllegalArgumentException("right dataset spec: " + m_params.m_rightDsSpec));
		String rightCols = splitter.remains()
								.getOrElseThrow(() -> new IllegalArgumentException("right dataset spec: " + m_params.m_rightDsSpec));
		
		return builder.loadHashJoin(leftDsId, leftCols, rightDsId, rightCols,
									m_opParams.m_joinOutCols, opts);
	}
}
