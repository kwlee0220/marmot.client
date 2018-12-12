package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.vavr.control.Option;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.plan.STScriptPlanLoader;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.StoreIntoDataSetProto;
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
			
			String script;
			if ( cl.getArgumentCount() > 0 ) {
				File file = new File(cl.getArgument("plan_file"));
				script = IOUtils.toString(file);
			}
			else {
				try ( InputStream is = System.in ) {
					script = IOUtils.toString(is, Charset.defaultCharset());
				}
			}
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
		String outDsId = cl.getOptionString("output").getOrNull();

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
				.ifPresent(blkSz -> optList.add(DataSetOption.BLOCK_SIZE(blkSz)));
			
			if ( cl.hasOption("compress") ) {
				optList.add(DataSetOption.COMPRESS);
			}
			
			String fromPlanDsId = getStoreTargetDataSetId(plan).getOrNull();
			if ( outDsId == null && fromPlanDsId == null ) {
				throw new IllegalArgumentException("result dataset id is messing");
			}
			else if ( outDsId == null ) {
				outDsId = fromPlanDsId;
			}
			else if ( outDsId != null && fromPlanDsId != null && !outDsId.equals(fromPlanDsId) ) {
				String details = String.format("target dataset id does not match: "
											+ "plan=%s, command_line=%s", fromPlanDsId, outDsId);
				throw new IllegalArgumentException(details);
			}

			marmot.createDataSet(outDsId, plan, Iterables.toArray(optList, DataSetOption.class));
		}
		else {
			plan = adjustPlanForStore(outDsId, plan);
			marmot.execute(plan);
		}
	}
	
	private static Option<String> getStoreTargetDataSetId(Plan plan) {
		OperatorProto last = plan.getLastOperator()
				.getOrElseThrow(() -> new IllegalArgumentException("plan is empty"));
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
				return Option.some(last.getStoreIntoDataset().getId());
			default:
				return Option.none();
		}
	}
	
	private static Plan adjustPlanForStore(String dsId, Plan plan) {
		OperatorProto last = plan.getLastOperator()
								.getOrElseThrow(() -> new IllegalArgumentException("plan is empty"));
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
			case STORE_AS_CSV:
			case STORE_INTO_JDBC_TABLE:
			case STORE_AND_RELOAD:
			case STORE_AS_HEAPFILE:
				return plan;
			default:
				StoreIntoDataSetProto store = StoreIntoDataSetProto.newBuilder()
																	.setId(dsId)
																	.build();
				OperatorProto op = OperatorProto.newBuilder().setStoreIntoDataset(store).build();
				return Plan.fromProto(plan.toProto()
											.toBuilder()
											.addOperators(op)
											.build());
		}
	}
}
