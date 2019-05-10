package marmot.geo.command;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagGeometry {
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_tag_geometry ");
		parser.addArgumentName("dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("col", "name", "match column name", true);
		parser.addArgOption("ref", "name", "reference dataset name", true);
		parser.addArgOption("ref_col", "name", "reference column name", true);
		parser.addArgOption("output", "name", "output dataset name", true);
		parser.addArgOption("geom_col", "name", "output geometry name (default: ref_col)", false);
		parser.addArgOption("workers", "count", "reduce task count", false);
		parser.addOption("help", "show usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		String dsId = cl.getArgument("dataset");
		String joinCol = cl.getString("col");
		String refDsId = cl.getString("ref");
		String refCol = cl.getString("ref_col");
		String outDs = cl.getString("output");
		FOption<Integer> nworkers = cl.getOptionInt("workers");
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		JoinOptions opts = new JoinOptions().workerCount(nworkers);

		DataSet ds = marmot.getDataSet(refDsId);
		if ( !ds.hasGeometryColumn() ) {
			System.err.println("reference dataset does not have default Geometry column: "
								+ "id=" + dsId);
			System.exit(-1);
		}
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		String outputGeomCol = cl.getOptionString("geom_col").getOrElse(gcInfo.name());

		String outputCols = String.format("param.%s as %s,*-{%s}", gcInfo.name(),
											outputGeomCol, outputGeomCol);
		Plan plan = marmot.planBuilder("tag_geometry")
								.load(dsId)
								.hashJoin(joinCol, refDsId, refCol, outputCols, opts)
								.store(outDs)
								.build();
		
		gcInfo = new GeometryColumnInfo(outputGeomCol, gcInfo.srid());
		marmot.createDataSet(outDs, plan, DataSetOption.GEOMETRY(gcInfo), DataSetOption.FORCE);
	}
}
