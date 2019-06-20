package marmot.command;

import java.io.IOException;

import com.vividsolutions.jts.io.ParseException;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
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
@Command(name="mc_spatial_join", description="execute spatial join based plan",
		mixinStandardHelpOptions=false)
public class RemoteSpatialJoinMain extends PlanBasedMarmotCommand {
	@Mixin SpatialJoinParams m_params;
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteSpatialJoinMain cmd = new RemoteSpatialJoinMain();
		CommandLine commandLine = new CommandLine(cmd);
		commandLine.parse(args);
		
		if ( commandLine.isUsageHelpRequested() ) {
			commandLine.usage(System.out, Ansi.OFF);
		}
		else {
			try {
				cmd.run("load_spatial_join", cmd.m_params.m_outputDsId);
			}
			catch ( Exception e ) {
				System.err.printf("failed: %s%n%n", e);
				commandLine.usage(System.out, Ansi.OFF);
			}
		}
	}
	
	@Override
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder)
		throws ParseException, IOException {
		DataSet left = marmot.getDataSet(m_params.m_leftDsId);
		if ( left.hasGeometryColumn() ) {
			setGeometryColumnInfo(left.getGeometryColumnInfo());
		}
		
		if ( m_params.m_outCols != null ) {
			return builder.loadSpatialIndexJoin(m_params.m_leftDsId, m_params.m_rightDsId,
												m_params.m_outCols);
		}
		else {
			return builder.loadSpatialIndexJoin(m_params.m_leftDsId, m_params.m_rightDsId,
												SpatialJoinOptions.EMPTY);
		}
	}
	
	private static class SpatialJoinParams {
		@Parameters(paramLabel="left_dataset", index="0", description={"left dataset id"})
		private String m_leftDsId;
		
		@Parameters(paramLabel="right_dataset", index="1", description={"right dataset id"})
		private String m_rightDsId;
		
		@Parameters(paramLabel="output_dataset", index="2", description={"output dataset id"})
		private String m_outputDsId;
		
		@Parameters(paramLabel="output_columns", arity="0..1", index="3",
					description={"output columns"})
		private String m_outCols;
	}
}
