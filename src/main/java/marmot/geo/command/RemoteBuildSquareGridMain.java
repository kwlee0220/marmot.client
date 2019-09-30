package marmot.geo.command;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.command.MarmotClientCommands;
import marmot.command.PlanBasedMarmotCommand;
import marmot.optor.geo.SquareGrid;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_build_square_grid",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="build a square-grid for a dataset")
public class RemoteBuildSquareGridMain extends PlanBasedMarmotCommand {
	@Mixin private Params m_params;
	
	public static class Params {
		@Parameters(paramLabel="dataset", index="0", arity="1..1",
					description={"dataset id for grid boundary"})
		private String m_input;
		
		@Parameters(paramLabel="grid_dataset", index="1", arity="1..1",
					description={"square-grid dataset"})
		private String m_output;

		@Option(names={"-grid"}, paramLabel="grid_expr", required=true, description={"grid expression"})
		public void setSquareGrid(String expr) {
			m_grid = SquareGrid.parseString(expr);
		}
		private SquareGrid m_grid;

		@Option(names={"-o", "-overlap"}, description={"skip un-overlapped cells)"})
		private boolean m_overlap = false;
	}
	
	public static final void main(String... args) {
		MarmotClientCommands.configureLog4j();

		RemoteBuildSquareGridMain cmd = new RemoteBuildSquareGridMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				
				cmd.run("spatial_join", cmd.m_params.m_output);
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder) {
		GeometryColumnInfo gcInfo = marmot.getDataSet(m_params.m_input)
											.getGeometryColumnInfo();
		setGeometryColumnInfo(gcInfo);
		String prjExpr = String.format("cell_geom as %s,cell_id,cell_pos", gcInfo.name());
		
		if ( m_params.m_overlap ) {
			return builder.load(m_params.m_input)
							.assignGridCell(gcInfo.name(), m_params.m_grid, false)
							.project(prjExpr)
							.distinct("cell_id");
		}
		else {
			return builder.loadGrid(m_params.m_grid);
		}
	}
}
