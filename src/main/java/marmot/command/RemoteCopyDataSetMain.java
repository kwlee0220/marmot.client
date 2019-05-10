package marmot.command;

import java.io.File;
import java.io.IOException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.geo.GeoClientUtils;
import marmot.type.MapTile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.CSV;
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
@Command(name="mc_copy", description="execute simple plan",
		mixinStandardHelpOptions=false)
public class RemoteCopyDataSetMain extends PlanBasedMarmotCommand {
	@Mixin Params m_params;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteCopyDataSetMain cmd = new RemoteCopyDataSetMain();
		CommandLine commandLine = new CommandLine(cmd);
		
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				try {
					cmd.run("copy_dataset", cmd.m_params.m_outputDsId);
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
	protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder)
		throws ParseException, IOException {
		DataSet input = marmot.getDataSet(m_params.m_inputDsId);
		if ( input.hasGeometryColumn() ) {
			setGeometryColumnInfo(input.getGeometryColumnInfo());
		}
		
		if ( m_params.m_rangeWkt != null ) {
			Geometry key = GeoClientUtils.fromWKT(m_params.m_rangeWkt);
			return builder.query(m_params.m_inputDsId, key);
		}
		else if ( m_params.m_rangePath != null ) {
			File wktFile = new File(m_params.m_rangePath);
			String wkt = IOUtils.toString(wktFile);
			Geometry key = GeoClientUtils.fromWKT(wkt);
			return builder.query(m_params.m_inputDsId, key);
		}
		else if ( m_params.m_rangeRect != null ) {
			double[] coords = CSV.parseCsv(m_params.m_rangeRect)
									.mapToDouble(Double::parseDouble)
									.toArray();
			Envelope range = new Envelope(new Coordinate(coords[0], coords[1]),
											new Coordinate(coords[2], coords[3]));
			Geometry key = GeoClientUtils.toPolygon(range);
			return builder.query(m_params.m_inputDsId, key);
		}
		else if ( m_params.m_rangeQuadKey != null ) {
			Envelope range = MapTile.fromQuadKey(m_params.m_rangeQuadKey).getBounds();
			Geometry key = GeoClientUtils.toPolygon(range);
			return builder.query(m_params.m_inputDsId, key);
		}
		else {
			return builder.load(m_params.m_inputDsId);
		}
	}
	
	private static class Params {
		@Parameters(paramLabel="input_dataset", index="0", description={"input dataset id"})
		private String m_inputDsId;
		
		@Parameters(paramLabel="output_dataset", index="1", description={"output dataset id"})
		private String m_outputDsId;
		
		@Option(names={"-range_file"}, paramLabel="path", description={"file path to WKT file"})
		String m_rangePath;
		
		@Option(names={"-range_wkt"}, paramLabel="wkt", description={"key geometry in WKT"})
		String m_rangeWkt;
		
		@Option(names={"-range_rect"}, paramLabel="rect", description={"rectangle coordinates"})
		String m_rangeRect;
		
		@Option(names={"-range_qkey"}, paramLabel="qkey", description={"range query with quad-key"})
		String m_rangeQuadKey;
	}
}
