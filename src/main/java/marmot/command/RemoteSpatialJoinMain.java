package marmot.command;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.command.MarmotClientCommands;
import marmot.command.PlanBasedMarmotCommand;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteSpatialJoinMain extends PlanBasedMarmotCommand {
	private RemoteSpatialJoinMain(PBMarmotClient marmot, CommandLine cl, String outDsId,
									GeometryColumnInfo gcInfo) {
		super(marmot, cl, outDsId, gcInfo);
	}
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		CommandLineParser parser = new CommandLineParser("mc_spatial_join ");
		parser.addArgumentName("left_dataset");
		parser.addArgumentName("right_dataset");
		parser.addArgumentName("output_dataset");
		parser.addArgumentName("[output_columns]");
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
			
			RemoteSpatialJoinMain join = new RemoteSpatialJoinMain(marmot, cl, outDsId, gcInfo);
			join.run();
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
	
	public void run() throws Exception {
		PlanBuilder builder = m_marmot.planBuilder("load_spatial_join");
		
		builder = addLoadSpatialJoin(builder);
		builder = appendOperators(builder);
		
		Plan plan = builder.build();
		run(plan);
	}
	
	private PlanBuilder addLoadSpatialJoin(PlanBuilder builder) {
		String leftDsId = m_cl.getArgument(0);
		String rightDsId = m_cl.getArgument(1);
		String outCols = (m_cl.getArgumentCount() > 3) ? m_cl.getArgument(3) : null;
		
		if ( outCols != null ) {
			return builder.loadSpatialIndexJoin(leftDsId, rightDsId, outCols);
		}
		else {
			return builder.loadSpatialIndexJoin(leftDsId, rightDsId);
		}
	}
}
