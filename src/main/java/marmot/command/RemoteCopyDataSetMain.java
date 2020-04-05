package marmot.command;

import java.io.IOException;
import java.util.Map;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.dataset.DataSet;
import marmot.geo.GeoClientUtils;
import marmot.plan.LoadOptions;
import marmot.plan.PredicateOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.Utilities;
import utils.func.FOption;


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
				cmd.run("copy_dataset", cmd.m_params.m_outputDsId);
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
		
		PredicateOptions opts = PredicateOptions.DEFAULT;
		String rangeExpr = m_params.m_range;
		if ( m_params.m_exRange != null ) {
			rangeExpr = m_params.m_exRange;
			opts = opts.negated(true);
		}
		if ( rangeExpr != null ) {
			Map<String,String> kvMap = Utilities.parseKeyValueMap(rangeExpr, ';');
			if ( kvMap.get("dataset") == null ) {
				return builder.query(m_params.m_inputDsId, kvMap.get("dataset"), opts);
			}
			if ( kvMap.get("bounds") == null ) {
				String boundsExpr = kvMap.get("bounds");
				Envelope bounds = GeoClientUtils.parseEnvelope(boundsExpr).get();
				return builder.query(m_params.m_inputDsId, bounds, opts);
			}
			
			throw new IllegalArgumentException("invalid range expression: " + rangeExpr);
		}
		else {
			LoadOptions loadOpts = m_params.m_mapperCount
										.map(cnt -> (cnt > 0) ? LoadOptions.FIXED_MAPPERS(cnt) :LoadOptions.MAPPERS())
										.getOrElse(LoadOptions.DEFAULT);
			return builder.load(m_params.m_inputDsId, loadOpts);
		}
	}
	
	private static class Params {
		@Parameters(paramLabel="input_dataset", index="0", description={"input dataset id"})
		private String m_inputDsId;
		
		@Parameters(paramLabel="output_dataset", index="1", description={"output dataset id"})
		private String m_outputDsId;
		
		@Option(names={"-range"}, paramLabel="range expr", description={"target range area"})
		String m_range;
		
		@Option(names={"-ex_range"}, paramLabel="range expr", description={"target range area"})
		String m_exRange;
		
		@Option(names="-mappers", paramLabel="count", description="number of mappers")
		public void setMapperCount(int count) {
			m_mapperCount = FOption.of(count);
		}
		private FOption<Integer> m_mapperCount = FOption.empty();
	}
}
