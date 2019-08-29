package marmot.command;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.CONVEX_HULL;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.ENVELOPE;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.UNION_GEOM;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.optor.StoreAsCsvOptions;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.StoreIntoDataSetProto;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import utils.CSV;
import utils.KeyValue;
import utils.UnitUtils;
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
abstract class PlanBasedMarmotCommand {
	@Mixin private MarmotConnector m_connector;
	@Mixin private OpParams m_opParams;
	@Mixin private StoreDataSetParameters m_storeParams;
	@Mixin private UsageHelp m_help;

	private MarmotRuntime m_marmot;
	private GeometryColumnInfo m_gcInfo;
	
	abstract protected PlanBuilder addLoad(MarmotRuntime marmot, PlanBuilder builder)
		throws Exception;
	
	public void run(String planName, String outputDsId) throws Exception {
		m_marmot = m_connector.connect();
		
		Plan plan = buildPlan(m_marmot, planName);
		if ( m_opParams.m_storeAsCsvOptions != null ) {
			plan = plan.toBuilder()
						.storeAsCsv(outputDsId, m_opParams.m_storeAsCsvOptions)
						.build();
			m_marmot.execute(plan);
		}
		else if ( !m_storeParams.getAppend() ) {
			String fromPlanDsId = getStoreTargetDataSetId(plan).getOrNull();
			if ( outputDsId == null && fromPlanDsId == null ) {
				throw new IllegalArgumentException("result dataset id is messing");
			}
			else if ( outputDsId == null ) {
				outputDsId = fromPlanDsId;
			}
			
			m_marmot.createDataSet(outputDsId, plan, m_storeParams.toOptions());
		}
		else {
			plan = adjustPlanForStore(outputDsId, plan);
			m_marmot.execute(plan);
		}
	}
	
	protected void setGeometryColumnInfo(GeometryColumnInfo gcInfo) {
		m_gcInfo = gcInfo;
	}

	protected Plan buildPlan(MarmotRuntime marmot, String planName) throws Exception {
		PlanBuilder builder = marmot.planBuilder(planName);
		builder = addLoad(marmot, builder);
		builder = appendOperators(builder);
		
		return builder.build();
	}
	
	protected void createDataSet(String outDsId, Plan plan) throws Exception {
		if ( !m_storeParams.getAppend() ) {
			m_marmot.createDataSet(outDsId, plan, m_storeParams.toOptions());
		}
		else {
			m_marmot.execute(plan);
		}
	}
	
	private PlanBuilder appendOperators(PlanBuilder builder) {
		builder = addCentroid(builder);
		builder = addBuffer(builder);
		builder = addAssignGridCell(builder);
		builder = addDefineColumn(builder);
		builder = addUpdate(builder);
		builder = addExpand(builder);
		builder = addFilter(builder);
		
		// 'spatial_join' 관련 인자가 있는 경우, spatial_join 연산을 추가한다.
		if ( m_opParams.m_spatialJoin != null ) {
			addSpatialJoin(builder);
		}
		else {
			// 'join' 관련 인자가 있는 경우, join 연산을 추가한다.
			addJoin(builder);
		}
		
		// 'groupBy' 관련 인자가 있는 경우, groupBy 연산을 추가한다.
		addGroupBy(builder);
		
		builder = addProject(builder);
		builder = addDistinct(builder);
		builder = addTransformCrs(builder);
		builder = addShard(builder);
		
		return builder;
	}
	
	private PlanBuilder addCentroid(PlanBuilder builder) {
		if ( m_opParams.m_centroid ) {
			if ( m_gcInfo == null ) {
				throw new IllegalArgumentException("dataset does not have default Geometry column");
			}

			builder = builder.centroid(m_gcInfo.name());
		}
		
		return builder;
	}
	
	private PlanBuilder addBuffer(PlanBuilder builder) {
		if ( m_opParams.m_bufferRadius > 0 ) {
			if ( m_gcInfo == null ) {
				throw new IllegalArgumentException("dataset does not have default Geometry column");
			}

			builder = builder.buffer(m_gcInfo.name(), m_opParams.m_bufferRadius);
		}
		
		return builder;
	}
	
	private PlanBuilder addAssignGridCell(PlanBuilder builder) {
		if ( m_opParams.m_grid != null ) {
			if ( m_gcInfo == null ) {
				throw new IllegalArgumentException("dataset does not have default Geometry column");
			}
			
			builder = builder.assignGridCell(m_gcInfo.name(), m_opParams.m_grid, false);
		}
		
		return builder;
	}
	
	private PlanBuilder addExpand(PlanBuilder builder) {
		if ( m_opParams.m_expandExpr != null ) {
			builder = builder.expand(m_opParams.m_expandExpr);
		}
		return builder;
	}
	
	private PlanBuilder addUpdate(PlanBuilder builder) {
		if ( m_opParams.m_updateExpr != null ) {
			builder = builder.update(m_opParams.m_updateExpr);
		}
		return builder;
	}
	
	private PlanBuilder addDefineColumn(PlanBuilder builder) {
		if ( m_opParams.m_defineColumnExpr != null ) {
			String colDecl = m_opParams.m_defineColumnExpr;
			
			int idx = m_opParams.m_defineColumnExpr.indexOf('=');
			if ( idx >= 0 ) {
				String initValue = m_opParams.m_defineColumnExpr.substring(idx+1).trim();
				
				String decl = m_opParams.m_defineColumnExpr.substring(0, idx).trim();
				idx = decl.indexOf(' ');
				if ( idx < 0 ) {
					throw new IllegalArgumentException("invalid define_column expr: " + m_opParams.m_defineColumnExpr);
				}
				String typeName = decl.substring(0, idx).trim();
				String colName = decl.substring(idx+1).trim();
				colDecl = String.format("%s:%s", colName, typeName);
				
				builder = builder.defineColumn(colDecl, initValue);
			}
			else {
				builder = builder.defineColumn(m_opParams.m_defineColumnExpr);
			}
		}
		
		return builder;
	}
	
	private PlanBuilder addFilter(PlanBuilder builder) {
		if ( m_opParams.m_filterExpr != null ) {
			builder = builder.filter(m_opParams.m_filterExpr);
		}
		return builder;
	}
	
	private PlanBuilder addSpatialJoin(PlanBuilder builder) {
		String join = m_opParams.m_spatialJoin;
		if ( join == null ) {
			return builder;
		}
		
		List<String> parts = CSV.parseCsv(join, ':', '\\').toList();
		if ( parts.size() != 2 ) {
			String details = String.format("invalid spatial_join argument: '%s'%n", join);
			throw new IllegalArgumentException(details);
		}
		String joinCols = parts.get(0);
		String paramDsId = parts.get(1);
		
		String outCols = m_opParams.m_joinOutCols;
		String joinType = FOption.ofNullable(m_opParams.m_joinType).getOrElse("inner");
		
		SpatialJoinOptions opts = parseSpatialJoinOptions();
		switch ( joinType ) {
			case "inner":
				return builder.spatialJoin(joinCols, paramDsId, opts);
			case "left_outer":
				return builder.spatialOuterJoin(joinCols, paramDsId, opts);
			case "semi":
				return builder.spatialSemiJoin(joinCols, paramDsId, opts);
			case "aggregate":
				AggregateFunction[] aggrs = parseAggregate();
				if ( aggrs == null ) {
					throw new IllegalArgumentException("aggregate join has no aggregation functions");
				}
				return builder.spatialAggregateJoin(joinCols, paramDsId, aggrs, opts);
			default:
				throw new IllegalArgumentException("'invalid spatial_join type: " + joinType);
		}
	}
	
	private PlanBuilder addJoin(PlanBuilder builder) {
		if ( m_opParams.m_join != null ) {
			List<String> parts = CSV.parseCsv(m_opParams.m_join, ':', '\\').toList();
			if ( parts.size() != 3 ) {
				String details = String.format("invalid join argument: 'join': '%s'",
												m_opParams.m_join);
				throw new IllegalArgumentException(details);
			}
			String joinCols = parts.get(0);
			String paramDsId = parts.get(1);
			String paramJoinCols = parts.get(2);
			
			JoinType joinType = FOption.ofNullable(m_opParams.m_joinType)
										.map(s -> s + "_join")
										.map(JoinType::fromString)
										.getOrElse(() -> JoinType.INNER_JOIN);
			JoinOptions opts = JoinOptions.create(joinType);
			
			if ( m_opParams.m_joinOutCols != null ) {
				return builder.hashJoin(joinCols, paramDsId, paramJoinCols,
									m_opParams.m_joinOutCols, opts);
			}
			else {
				throw new IllegalArgumentException("'join_output_col' is not present");
			}
		}
		else {
			return builder;
		}
	}
	
	private PlanBuilder addGroupBy(PlanBuilder builder) {
		AggregateFunction[] aggrs = parseAggregate();

		if ( m_opParams.m_groupBy != null ) {
			Group group = Group.parseGroup(m_opParams.m_groupBy);
			
			if ( aggrs != null ) {
				return builder.aggregateByGroup(group, aggrs);
			}
			else if ( m_opParams.m_takeCount > 0 ) {
				return builder.takeByGroup(group, m_opParams.m_takeCount);
			}
			
			throw new IllegalArgumentException("no aggregation for GroupBy");
		}
		else if ( aggrs != null ) {
			return builder.aggregate(aggrs);
		}
		else {
			return builder;
		}
	}
	
	private SpatialJoinOptions parseSpatialJoinOptions() {
		SpatialJoinOptions opts = SpatialJoinOptions.EMPTY;
		if ( m_opParams.m_joinOutCols != null ) {
			opts = opts.outputColumns(m_opParams.m_joinOutCols);
		}
		
		SpatialRelation rel = FOption.ofNullable(m_opParams.m_joinExpr)
									.map(String::toLowerCase)
									.map(SpatialRelation::parse)
									.getOrElse(SpatialRelation.INTERSECTS);
		switch ( rel.getCode() ) {
			case CODE_INTERSECTS:
			case CODE_WITHIN_DISTANCE:
				opts = opts.joinExpr(rel);
				break;
			default:
				throw new IllegalArgumentException("unknown spatial_join_expression: " + rel);
		}
		
		return opts;
	}
	
	private AggregateFunction[] parseAggregate() {
		if ( m_opParams.m_aggregates == null ) {
			return null;
		}

		List<AggregateFunction> aggrs = Lists.newArrayList();
		for ( String aggrSpecStr: CSV.parseCsv(m_opParams.m_aggregates, ',', '\\').toList() ) {
			List<String> aggrSpec = CSV.parseCsv(aggrSpecStr, ':', '\\').toList();
			
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
						throw new IllegalArgumentException("SUM: target column is not specified");
					}
					break;
				case "AVG":
					if ( aggrSpec.size() == 2 ) {
						aggr = AVG(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("AVG: target column is not specified");
					}
					break;
				case "MAX":
					if ( aggrSpec.size() == 2 ) {
						aggr = MAX(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("MAX: target column is not specified");
					}
					break;
				case "MIN":
					if ( aggrSpec.size() == 2 ) {
						aggr = MIN(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("MIN: target column is not specified");
					}
					break;
				case "STDDEV":
					if ( aggrSpec.size() == 2 ) {
						aggr = STDDEV(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("STDDEV: target column is not specified");
					}
					break;
				case "CONVEX_HULL":
					if ( aggrSpec.size() == 2 ) {
						aggr = CONVEX_HULL(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("CONVEX_HULL: target column is not specified");
					}
					break;
				case "ENVELOPE":
					if ( aggrSpec.size() == 2 ) {
						aggr = ENVELOPE(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("ENVELOPE: target column is not specified");
					}
					break;
				case "GEOM_UNION":
					if ( aggrSpec.size() == 2 ) {
						aggr = UNION_GEOM(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("GEOM_UNION: target column is not specified");
					}
					break;
				default:
					String details = String.format("invalid aggregation function: %s'%n", aggrSpec.get(0));
					throw new IllegalArgumentException(details);
					
			}
			if ( aggrSpec.size() >= 3 ) {
				aggr = aggr.as(aggrSpec.get(2));
			}
			aggrs.add(aggr);
		}
		
		return Iterables.toArray(aggrs, AggregateFunction.class);
	}
	
	private PlanBuilder addProject(PlanBuilder builder) {
		if ( m_opParams.m_projectExpr != null ) {
			builder = builder.project(m_opParams.m_projectExpr);
		}
		
		return builder;
	}
	
	private PlanBuilder addDistinct(PlanBuilder builder) {
		if ( m_opParams.m_distinctCols != null ) {
			builder = builder.distinct(m_opParams.m_distinctCols);
		}
		
		return builder;
	}
	
	private PlanBuilder addTransformCrs(PlanBuilder builder) {
		if ( m_opParams.m_transformSrid != null ) {
			if ( m_gcInfo == null ) {
				throw new IllegalArgumentException("dataSet does not have default Geometry column");
			}
			
			if ( !m_gcInfo.srid().equals(m_opParams.m_transformSrid) ) {
				builder = builder.transformCrs(m_gcInfo.name(), m_gcInfo.srid(),
												m_opParams.m_transformSrid);
				
				m_gcInfo = new GeometryColumnInfo(m_gcInfo.name(), m_opParams.m_transformSrid);
			}
		}
		
		return builder;
	}
	
	private PlanBuilder addShard(PlanBuilder builder) {
		if ( m_opParams.m_shardCount > 0 ) {
			builder = builder.shard(m_opParams.m_shardCount);
		}
		return builder;
	}
	
	private static class OpParams {
		@Option(names={"-centroid"}, description={"perform centroid on the geometry column"})
		boolean m_centroid;
		
		double m_bufferRadius = -1;
		@Option(names={"-buffer"}, paramLabel="meter", description={"buffer radius"})
		public void setBuffer(String distStr) {
			m_bufferRadius = UnitUtils.parseLengthInMeter(distStr);
		}
		
		@Option(names={"-assign_gridcell"}, paramLabel="grid",
				description={"grid-cell information"})
		private void setSquareGrid(String gridExpr) {
			m_grid = SquareGrid.parseString(gridExpr);
		}
		SquareGrid m_grid;
		
		@Option(names={"-expand"}, paramLabel="expr", description={"expand expression"})
		String m_expandExpr;
		
		@Option(names={"-update"}, paramLabel="expr", description={"update expression"})
		String m_updateExpr;
		
		@Option(names={"-define_column"}, paramLabel="expr", description={"define a column"})
		String m_defineColumnExpr;
		
		@Option(names={"-filter"}, paramLabel="expr", description={"filter expression"})
		String m_filterExpr;
		
		@Option(names={"-sample"}, paramLabel="ratio", description={"sampling ratio"})
		double m_sampleRatio;
		
		//------------------------------------------------------------------------------

		@Option(names={"-spatial_join"}, paramLabel="cols:dsid",
				description={"join columns, parameter join dataset"})
		String m_spatialJoin;
		
		@Option(names={"-join"}, paramLabel="cols:dsid:join_cols",
				description={"join columns, parameter join dataset, and columns"})
		String m_join;
		
		@Option(names={"-join_type"}, paramLabel="type",
				description={"join type: inner|left_outer|right_outer|full_outer|semi|aggregate (default: inner)"})
		String m_joinType;
		
		@Option(names={"-join_expr"}, paramLabel="expr",
				description={"join expression. (eg: within_distance(15)"})
		String m_joinExpr;
		
		@Option(names={"-join_output_cols"}, paramLabel="cols",
				description={"join output columns"})
		String m_joinOutCols;
		
		//------------------------------------------------------------------------------
		
		@Option(names={"-group_by"}, paramLabel="cols:tags",
				description={"groupping columns (and optionally tag columns)"})
		String m_groupBy;
		
		@Option(names={"-aggregate"}, paramLabel="funcs", description={"aggregate functions)"})
		String m_aggregates;
		
		@Option(names={"-take"}, paramLabel="count", description={"take count"})
		int m_takeCount = -1;
		
		//------------------------------------------------------------------------------
		
		@Option(names={"-project"}, paramLabel="colums", description={"target column list"})
		String m_projectExpr;
		
		@Option(names={"-distinct"}, paramLabel="columns", description={"distinct key columns"})
		String m_distinctCols;
		
		@Option(names={"-transform_srid"}, paramLabel="EPSG-code",
				description={"destination SRID for transformCRS"})
		String m_transformSrid;
		
		@Option(names={"-shard"}, paramLabel="count", description={"reducer count"})
		int m_shardCount = -1;

		@Option(names={"-store_as_csv"}, paramLabel="csv_info",
				description={"csv options for result csv files"})
		private void setStoreAsCsv(String expr) {
			m_storeAsCsvOptions = StoreAsCsvOptions.DEFAULT().quote('"');
			
			for ( KeyValue<String,String> kv: Utilities.parseKeyValues(expr) ) {
				switch ( kv.key().toLowerCase() ) {
					case "delim":
						m_storeAsCsvOptions = m_storeAsCsvOptions.delimiter(kv.value().charAt(0));
						break;
					case "quote":
						m_storeAsCsvOptions = m_storeAsCsvOptions.quote(kv.value().charAt(0));
						break;
					case "escape":
						m_storeAsCsvOptions = m_storeAsCsvOptions.escape(kv.value().charAt(0));
						break;
					case "charset":
						m_storeAsCsvOptions = m_storeAsCsvOptions.charset(kv.value());
						break;
					case "blocksize":
						m_storeAsCsvOptions = m_storeAsCsvOptions.blockSize(UnitUtils.parseByteSize(kv.value()));
						break;
					case "compression":
						m_storeAsCsvOptions = m_storeAsCsvOptions.compressionCodecName(kv.value());
						break;
				}
			}
		}
		private StoreAsCsvOptions m_storeAsCsvOptions;
	}
	
	private static FOption<String> getStoreTargetDataSetId(Plan plan) {
		OperatorProto last = plan.getLastOperator()
								.getOrElseThrow(() -> new IllegalArgumentException("plan is empty"));
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
				return FOption.of(last.getStoreIntoDataset().getId());
			default:
				return FOption.empty();
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
