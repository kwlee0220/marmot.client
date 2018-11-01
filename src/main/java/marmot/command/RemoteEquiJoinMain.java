package marmot.command;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteEquiJoinMain extends PlanBasedMarmotCommand {
	private RemoteEquiJoinMain(PBMarmotClient marmot, CommandLine cl, String outDsId,
									GeometryColumnInfo gcInfo) {
		super(marmot, cl, outDsId, gcInfo);
	}
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		CommandLineParser parser = new CommandLineParser("mc_join ");
		parser.addArgumentName("left_dataset");
		parser.addArgumentName("left_join_cols");
		parser.addArgumentName("right_dataset");
		parser.addArgumentName("right_join_cols");
		parser.addArgumentName("output_dataset");
		parser.addArgumentName("output_columns");
		parser.addArgumentName("[join_type]");
		parser.addArgumentName("[nworkers]");
		PlanBasedMarmotCommand.setCommandLineParser(parser);

		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			if ( cl.getArgumentCount() < 3 ) {
				System.err.println("insufficient arguments");
				cl.exitWithUsage(-1);
			}
			
			// 원격 MarmotServer에 접속.
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			String outDsId = cl.getArgument("output_dataset");
			DataSet left = marmot.getDataSet(cl.getArgument(0));
			GeometryColumnInfo gcInfo = left.getGeometryColumnInfo();
			
			RemoteEquiJoinMain join = new RemoteEquiJoinMain(marmot, cl, outDsId, gcInfo);
			join.run();
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
	
	public void run() throws Exception {
		PlanBuilder builder = m_marmot.planBuilder("join");
		
		builder = addLoadJoin(builder);
		builder = appendOperators(builder);
		
		Plan plan = builder.build();
		run(plan);
	}
	
	private PlanBuilder addLoadJoin(PlanBuilder builder) {
		String leftDsId = m_cl.getArgument(0);
		String leftJoinCols = m_cl.getArgument(1);
		String rightDsId = m_cl.getArgument(2);
		String rightJoinCols = m_cl.getArgument(3);
		String outCols = m_cl.getArgument(4);
		
		String joinTypeStr = m_cl.getArgumentCount() > 5 ? m_cl.getArgument(5) : "inner";
		JoinType joinType = JoinType.fromString(joinTypeStr + "_join"); 
		JoinOptions opts = new JoinOptions().joinType(joinType);
		if ( m_cl.getArgumentCount() > 6) {
			opts = opts.workerCount(Integer.parseInt(m_cl.getArgument(6)));
		}
		
		return builder.loadEquiJoin(leftDsId, leftJoinCols, rightDsId, rightJoinCols,
									outCols, opts);
	}
}
