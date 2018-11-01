package marmot.command;

import java.io.File;
import java.io.IOException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.io.IOUtils;


/**
 * 연산 적용 순서는 다음과 같다.
 * <ol>
 * 	<li> range query
 * 	<li> centroid
 * 	<li> buffer
 * 	<li> filter
 * 	<li> spatial_join
 * 	<li> join
 * 	<li> group_by
 * 	<li> project
 * 	<li> sample
 * 	<li> transformCrs
 * 	<li> shard
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CopyDataSetMain extends PlanBasedMarmotCommand {
	private DataSet m_input;
	
	private CopyDataSetMain(PBMarmotClient marmot, CommandLine cl, DataSet input, String outDsId,
							GeometryColumnInfo gcInfo) {
		super(marmot, cl, outDsId, gcInfo);
		
		m_input = input;
	}
	
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		CommandLineParser parser = new CommandLineParser("mc_copy ");
		parser.addArgumentName("src_dataset");
		parser.addArgumentName("output_dataset");
		PlanBasedMarmotCommand.setCommandLineParser(parser);

		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			// 원격 MarmotServer에 접속.
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			String inDsId = cl.getArgument("src_dataset");
			String outDsId = cl.getArgument("output_dataset");
			DataSet input = marmot.getDataSet(inDsId);
			GeometryColumnInfo gcInfo = input.hasGeometryColumn()
										? input.getGeometryColumnInfo() : null;
			
			CopyDataSetMain copy = new CopyDataSetMain(marmot, cl, input, outDsId, gcInfo);
			copy.run();
		}
		catch ( Exception e ) {
			System.err.println(e);
			parser.exitWithUsage(-1);
		}
	}
	
	public void run() throws Exception {
		PlanBuilder builder = m_marmot.planBuilder("copy_dataset");
		
		builder = addLoad(builder);
		builder = appendOperators(builder);
		
		Plan plan = builder.build();
		run(plan);
	}
	
	private PlanBuilder addLoad(PlanBuilder builder)
		throws ParseException, IOException {
		Option<String> rangePath = m_cl.getOptionString("range_file");
		Option<String> rangeWkt = m_cl.getOptionString("range_wkt");
		
		if ( rangeWkt.isDefined() ) {
			Geometry key = GeoClientUtils.fromWKT(rangeWkt.get());
			return builder.query(m_input.getId(), SpatialRelation.INTERSECTS, key);
		}
		else if ( rangePath.isDefined() ) {
			File wktFile = new File(rangePath.get());
			String wkt = IOUtils.toString(wktFile);
			Geometry key = GeoClientUtils.fromWKT(wkt);
			return builder.query(m_input.getId(), SpatialRelation.INTERSECTS, key);
		}
		else {
			return builder.load(m_input.getId());
		}
	}
}
