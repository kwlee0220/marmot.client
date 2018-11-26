package marmot.command;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.CONVEX_HULL;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.ENVELOPE;
import static marmot.optor.AggregateFunction.GEOM_UNION;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;

import io.vavr.control.Option;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.PlanBuilder.GroupByPlanBuilder;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.optor.geo.SpatialRelation;
import marmot.plan.SpatialJoinOption;
import utils.CSV;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;


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
class PlanBasedMarmotCommand {
	protected final MarmotRuntime m_marmot;
	protected final CommandLine m_cl;
	protected final String m_outputDsId;
	
	protected GeometryColumnInfo m_gcInfo;
	
	protected PlanBasedMarmotCommand(MarmotRuntime marmot, CommandLine cl, String outDsId,
									GeometryColumnInfo gcInfo) {
		m_marmot = marmot;
		m_cl = cl;
		m_outputDsId = outDsId;
		m_gcInfo = gcInfo;
	}
	
	static final void setCommandLineParser(CommandLineParser parser) {
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("centroid", "perform centroid on the geometry column", false);
		parser.addArgOption("buffer", "meter", "buffer radius", false);
		parser.addArgOption("expand", "expr", "expand expression", false);
		parser.addArgOption("update", "expr", "update expression", false);
		parser.addArgOption("filter", "expr", "filter expression", false);
		parser.addArgOption("spatial_join", "cols:dsid",
							"join columns, parameter join dataset", false);
		parser.addArgOption("join", "cols:dsid:join_cols",
							"join columns, parameter join dataset, and columns", false);
		parser.addArgOption("join_output_cols", "cols", "join output columns", false);
		parser.addArgOption("join_type", "type",
							"join type: inner|left_outer|right_outer|full_outer|semi|aggregate (default: inner)", false);
		parser.addArgOption("join_expr", "expr", "join expression. (eg: within_distance(15)", false);
		parser.addArgOption("group_by", "cols:tags",
							"groupping columns (and optionally tag columns)", false);
		parser.addArgOption("aggregate", "funcs", "aggregate functions", false);
		parser.addArgOption("project", "colums", "target column list", false);
		parser.addArgOption("sample", "count", "target sample count", false);
		parser.addArgOption("shard", "count", "partition count", false);
		parser.addArgOption("dest_srid", "EPSG_code", "destination EPSG code", false);
		parser.addArgOption("block_size", "nbytes",
							"block size (default: source file block_size)", false);
		parser.addOption("compress", "compress output dataset");
		parser.addOption("a", "append into the existing dataset");
		parser.addOption("p", "print plan");
		parser.addOption("f", "delete the output dataset if it exists already", false);
		parser.addOption("h", "help usage");
	}
	
	public void run(Plan plan) throws Exception {
		boolean force = m_cl.hasOption("f");
		if ( force ) {
			m_marmot.deleteDataSet(m_outputDsId);
		}
		
		if ( m_cl.hasOption("p") ) {
			System.out.println(JsonFormat.printer().print(plan.toProto()));
			return;
		}
		
		createDataSet(plan);
	}
	
	protected PlanBuilder appendOperators(PlanBuilder builder) throws Exception {
		builder = addCentroid(builder);
		builder = addBuffer(builder);
		builder = addExpand(builder);
		builder = addUpdate(builder);
		builder = addFilter(builder);
		
		// 'spatial_join' 관련 인자가 있는 경우, spatial_join 연산을 추가한다.
		builder = addSpatialJoin(builder);
		
		// 'join' 관련 인자가 있는 경우, join 연산을 추가한다.
		builder = addJoin(builder);
		
		// 'groupBy' 관련 인자가 있는 경우, groupBy 연산을 추가한다.
		builder = addGroupBy(builder);
		
		builder = addProject(builder);
		
		builder = addTransformCrs(builder);
		builder = addShard(builder);
		
		return builder;
	}
	
	private PlanBuilder addCentroid(PlanBuilder builder) {
		if ( m_cl.hasOption("centroid") ) {
			if ( m_gcInfo == null ) {
				System.err.println("dataset does not have default Geometry column");
				m_cl.exitWithUsage(-1);
			}

			builder = builder.centroid(m_gcInfo.name());
		}
		
		return builder;
	}
	
	private PlanBuilder addBuffer(PlanBuilder builder) {
		double dist = m_cl.getOptionDouble("buffer").getOrElse(-1d);
		if ( dist > 0 ) {
			if ( m_gcInfo == null ) {
				System.err.println("dataset does not have default Geometry column");
				m_cl.exitWithUsage(-1);
			}

			builder = builder.buffer(m_gcInfo.name(), dist);
		}
		
		return builder;
	}
	
	private PlanBuilder addExpand(PlanBuilder builder) {
		String expr = m_cl.getOptionString("expand").getOrNull();
		if ( expr == null ) {
			return builder;
		}
		
		return builder.expand(expr);
	}
	
	private PlanBuilder addUpdate(PlanBuilder builder) {
		String expr = m_cl.getOptionString("update").getOrNull();
		if ( expr == null ) {
			return builder;
		}
		
		return builder.update(expr);
	}
	
	private PlanBuilder addFilter(PlanBuilder builder) {
		String expr = m_cl.getOptionString("filter").getOrNull();
		
		if ( expr != null ) {
			builder = builder.filter(expr);
		}
		return builder;
	}
	
	private PlanBuilder addSpatialJoin(PlanBuilder builder) {
		String join = m_cl.getOptionString("spatial_join").getOrNull();
		if ( join == null ) {
			return builder;
		}
		
		String outCols = m_cl.getOptionString("join_output_cols").getOrNull();
		String joinType = m_cl.getOptionString("join_type").getOrElse("inner");
		
		List<String> parts = CSV.parse(join, ':', '\\');
		if ( parts.size() != 2 ) {
			System.err.printf("invalid spatial_join argument: '%s'%n", join);
			m_cl.exitWithUsage(-1);
		}
		String joinCols = parts.get(0);
		String paramDsId = parts.get(1);
		
		SpatialJoinOption[] opts = parseSpatialJoinOption();
		
		switch ( joinType ) {
			case "inner":
				if ( outCols != null ) {
					return builder.spatialJoin(joinCols, paramDsId, outCols, opts);
				}
				else {
					return builder.spatialJoin(joinCols, paramDsId, opts);
				}
			case "left_outer":
				if ( outCols != null ) {
					return builder.spatialOuterJoin(joinCols, paramDsId, outCols, opts);
				}
				else {
					return builder.spatialOuterJoin(joinCols, paramDsId, opts);
				}
			case "semi":
				return builder.spatialSemiJoin(joinCols, paramDsId, opts);
			case "aggregate":
				AggregateFunction[] aggrs = parseAggregate();
				if ( aggrs == null ) {
					System.err.printf("aggregate join has no aggregation functions");
					m_cl.exitWithUsage(-1);
				}
				return builder.spatialAggregateJoin(joinCols, paramDsId, aggrs, opts);
			default:
				System.err.printf("'invalid spatial_join type: " + joinType);
				m_cl.exitWithUsage(-1);
				
		}
		
		return null;
	}
	
	private PlanBuilder addJoin(PlanBuilder builder) {
		Option<String> join = m_cl.getOptionString("join");
		Option<String> joinOutputs = m_cl.getOptionString("join_output_cols");
		Option<String> joinTypeStr = m_cl.getOptionString("join_type");

		if ( join.isDefined() ) {
			List<String> parts = CSV.parse(join.get(), ':', '\\');
			if ( parts.size() != 3 ) {
				System.err.printf("invalid join argument: 'join': '%s'%n", join.get());
				m_cl.exitWithUsage(-1);
			}
			String joinCols = parts.get(0);
			String paramDsId = parts.get(1);
			String paramJoinCols = parts.get(2);
			
			JoinType joinType = joinTypeStr.map(s -> s + "_join")
											.map(JoinType::fromString)
											.getOrElse(() -> JoinType.INNER_JOIN);
			JoinOptions opts = new JoinOptions().joinType(joinType);
			
			if ( joinOutputs.isDefined() ) {
				String outCols = joinOutputs.get();
				return builder.join(joinCols, paramDsId, paramJoinCols, outCols, opts);
			}
			else {
				System.err.printf("'join_output_col' is not present");
				m_cl.exitWithUsage(-1);
			}
		}
		
		return builder;
	}
	
	private PlanBuilder addGroupBy(PlanBuilder builder) {
		Option<String> grpByStr = m_cl.getOptionString("group_by");
		AggregateFunction[] aggrs = parseAggregate();

		if ( grpByStr.isDefined() ) {
			List<String> parts = CSV.parse(grpByStr.get(), ':', '\\');
			
			GroupByPlanBuilder grpBuilder = builder.groupBy(parts.get(0));
			if ( parts.size() > 1 ) {
				grpBuilder = grpBuilder.tagWith(parts.get(1));
			}
			
			if ( aggrs == null ) {
				System.err.printf("no aggregation for GroupBy'%n");
				m_cl.exitWithUsage(-1);
			}

			return grpBuilder.aggregate(aggrs);
		}
		else if ( aggrs != null ) {
			return builder.aggregate(aggrs);
		}
		else {
			return builder;
		}
	}
	
	private SpatialJoinOption[] parseSpatialJoinOption() {
		String joinExpr = m_cl.getOptionString("join_expr")
								.map(String::toLowerCase).getOrElse("intersects");
		
		SpatialRelation rel = SpatialRelation.parse(joinExpr);
		switch ( rel.getCode() ) {
			case CODE_INTERSECTS:
			case CODE_WITHIN_DISTANCE:
				return new SpatialJoinOption[] {SpatialJoinOption.JOIN_EXPR(rel)};
			default:
				System.err.printf("unknown spatial_join_expression: %s'%n", rel);
				m_cl.exitWithUsage(-1);
				return null;
		}
	}
	
	private AggregateFunction[] parseAggregate() {
		String aggrsStr = m_cl.getOptionString("aggregate").getOrNull();
		
		if ( aggrsStr == null ) {
			return null;
		}

		List<AggregateFunction> aggrs = Lists.newArrayList();
		for ( String aggrSpecStr: CSV.parse(aggrsStr, ',', '\\') ) {
			List<String> aggrSpec = CSV.parse(aggrSpecStr, ':', '\\');
			
			AggregateFunction aggr = null;
			switch ( aggrSpec.get(0).toUpperCase()) {
				case "COUNT":
					aggr = COUNT();
					break;
				case "SUM":
					if ( aggrSpec.size() == 2 ) {
						aggr = SUM(aggrSpec.get(1));
					}
					else {
						System.err.printf("SUM: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "AVG":
					if ( aggrSpec.size() == 2 ) {
						aggr = AVG(aggrSpec.get(1));
					}
					else {
						System.err.printf("AVG: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "MAX":
					if ( aggrSpec.size() == 2 ) {
						aggr = MAX(aggrSpec.get(1));
					}
					else {
						System.err.printf("MAX: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "MIN":
					if ( aggrSpec.size() == 2 ) {
						aggr = MIN(aggrSpec.get(1));
					}
					else {
						System.err.printf("MIN: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "STDDEV":
					if ( aggrSpec.size() == 2 ) {
						aggr = STDDEV(aggrSpec.get(1));
					}
					else {
						System.err.printf("STDDEV: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "CONVEX_HULL":
					if ( aggrSpec.size() == 2 ) {
						aggr = CONVEX_HULL(aggrSpec.get(1));
					}
					else {
						System.err.printf("CONVEX_HULL: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "ENVELOPE":
					if ( aggrSpec.size() == 2 ) {
						aggr = ENVELOPE(aggrSpec.get(1));
					}
					else {
						System.err.printf("ENVELOPE: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				case "GEOM_UNION":
					if ( aggrSpec.size() == 2 ) {
						aggr = GEOM_UNION(aggrSpec.get(1));
					}
					else {
						System.err.printf("GEOM_UNION: target column is not specified%n");
						m_cl.exitWithUsage(-1);
					}
					break;
				default:
					System.err.printf("invalid aggregation function: %s'%n", aggrSpec.get(0));
					m_cl.exitWithUsage(-1);
					
			}
			if ( aggrSpec.size() >= 3 ) {
				aggr = aggr.as(aggrSpec.get(2));
			}
			aggrs.add(aggr);
		}
		
		return Iterables.toArray(aggrs, AggregateFunction.class);
	}
	
	private PlanBuilder addProject(PlanBuilder builder) {
		String expr = m_cl.getOptionString("project").getOrNull();
		
		if ( expr != null ) {
			builder = builder.project(expr);
		}
		return builder;
	}
	
	private PlanBuilder addTransformCrs(PlanBuilder builder) {
		String destSrid = m_cl.getOptionString("dest_srid").getOrNull();

		if ( destSrid != null ) {
			if ( m_gcInfo == null ) {
				System.err.println("dataSet does not have default Geometry column");
				m_cl.exitWithUsage(-1);
			}
			
			if ( !m_gcInfo.srid().equals(destSrid) ) {
				builder = builder.transformCrs(m_gcInfo.name(), m_gcInfo.srid(), destSrid);
				
				m_gcInfo = new GeometryColumnInfo(m_gcInfo.name(), destSrid);
			}
		}
		
		return builder;
	}
	
	private PlanBuilder addShard(PlanBuilder builder) {
		int nparts = m_cl.getOptionInt("shard").getOrElse(-1);
		
		if ( nparts > 0 ) {
			builder = builder.shard(nparts);
		}
		return builder;
	}
	
	private void createDataSet(Plan plan) {
		boolean append = m_cl.hasOption("a");		
		if ( !append ) {
			List<DataSetOption> optList = Lists.newArrayList();
			
			if ( m_gcInfo != null ) {
				RecordSchema outSchema = m_marmot.getOutputRecordSchema(plan);
				if ( outSchema.existsColumn(m_gcInfo.name()) ) {
					optList.add(DataSetOption.GEOMETRY(m_gcInfo));
				}
			}
			
			if ( m_cl.hasOption("f") ) {
				optList.add(DataSetOption.FORCE);
			}
			
			m_cl.getOptionString("block_size")
				.map(UnitUtils::parseByteSize)
				.forEach(blkSz -> optList.add(DataSetOption.BLOCK_SIZE(blkSz)));
			
			if ( m_cl.hasOption("compress") ) {
				optList.add(DataSetOption.COMPRESS);
			}

			m_marmot.createDataSet(m_outputDsId, plan, Iterables.toArray(optList,
									DataSetOption.class));
		}
		else {
			m_marmot.execute(plan);
		}
	}
}
