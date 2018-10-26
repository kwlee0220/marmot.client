package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.plan.STScriptPlanLoader;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteRunPlanMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		CommandLineParser parser = new CommandLineParser("mc_run ");
		parser.addArgumentName("plan_file");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("output", "dataset id", "dataset id for the result", true);
		parser.addArgOption("geom_col", "name", "default Geometry column", false);
		parser.addArgOption("srid", "EPSG_code", "destination EPSG code", false);
		parser.addOption("p", "display plan", false);
		parser.addArgOption("block_size", "nbytes",
							"block size (default: source file block_size)", false);
		parser.addOption("compress", "compress output dataset");
		parser.addOption("f", "delete the output dataset if it exists already", false);
		parser.addOption("a", "append into the existing dataset");
		parser.addOption("h", "help usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = MarmotClientCommands.connect(cl);
			
			File file = new File(cl.getArgument("plan_file"));
			String script = IOUtils.toString(file);
//			if ( cl.hasOptionValue("file") ) {
//				script = IOUtils.toString(cl.getArgument("plan_file"));
//			}
//			else if ( cl.hasOptionValue("resource") ) {
//				String name = cl.getString("resource");
//				try ( InputStream is = Thread.currentThread()
//											.getContextClassLoader()
//											.getResourceAsStream(name) ) {
//					if ( is == null ) {
//						throw new IllegalArgumentException("invalid STScript: resource name=" + name);
//					}
//					script = IOUtils.toString(is, Charset.defaultCharset());
//				}
//			}
//			else {
//				try ( InputStream is = System.in ) {
//					script = IOUtils.toString(is, Charset.defaultCharset());
//				}
//			}
			String planJson = STScriptPlanLoader.toJson(script);
			
			boolean showPlan = cl.hasOption("p");
			if ( showPlan ) {
				System.out.println(planJson);
			}
			
			Plan plan = Plan.parseJson(planJson);
			createDataSet(marmot, plan, cl);
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
	
	private static void createDataSet(MarmotRuntime marmot, Plan plan, CommandLine cl) {
		String geomCol = cl.getOptionString("geom_col").getOrNull();
		String srid = cl.getOptionString("srid").getOrNull();
		
		boolean append = cl.hasOption("a");		
		if ( !append ) {
			List<DataSetOption> optList = Lists.newArrayList();
			
			if ( geomCol != null ) {
				RecordSchema outSchema = marmot.getOutputRecordSchema(plan);
				if ( outSchema.existsColumn(geomCol) ) {
					GeometryColumnInfo gcInfo = new GeometryColumnInfo(geomCol, srid);
					optList.add(DataSetOption.GEOMETRY(gcInfo));
				}
			}
			
			if ( cl.hasOption("f") ) {
				optList.add(DataSetOption.FORCE);
			}
			
			cl.getOptionString("block_size")
				.map(UnitUtils::parseByteSize)
				.forEach(blkSz -> optList.add(DataSetOption.BLOCK_SIZE(blkSz)));
			
			if ( cl.hasOption("compress") ) {
				optList.add(DataSetOption.COMPRESS);
			}

			String outDsId = cl.getString("output");
			marmot.createDataSet(outDsId, plan, Iterables.toArray(optList, DataSetOption.class));
		}
		else {
			marmot.execute(plan);
		}
	}
}
