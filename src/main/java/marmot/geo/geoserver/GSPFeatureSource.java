package marmot.geo.geoserver;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Predicate;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.geo.CRSUtils;
import marmot.geo.GeoClientUtils;
import marmot.geo.geotools.GeoToolsUtils;
import marmot.geo.geotools.MarmotFeatureIterator;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SpatialRelation;
import marmot.rset.RecordSets;
import marmot.support.DataSetPartitionCache;
import utils.Throwables;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPFeatureSource extends ContentFeatureSource {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPFeatureSource.class);
	private static final int MAX_CACHING_CLUSTERS = 5;

	private final MarmotRuntime m_marmot;
	private final GSPDataSetInfo m_dsInfo;
	private final String m_dsId;
	private final DataSetPartitionCache m_cache;
	private final GeometryColumnInfo m_gcInfo;
	private final Lazy<ReferencedEnvelope> m_mbr;
	private final CoordinateReferenceSystem m_crs;
	private Option<Long> m_sampleCount = Option.none();
	
	GSPFeatureSource(ContentEntry entry, GSPDataSetInfo info, DataSetPartitionCache cache) {
		super(entry, Query.ALL);
		
		m_marmot = info.getMarmotRuntime();
		m_dsInfo = info;
		m_dsId = m_dsInfo.getDataSet().getId();
		m_gcInfo = m_dsInfo.getDataSet().getGeometryColumnInfo();
		m_crs = CRSUtils.toCRS(m_gcInfo.srid());
		m_cache = cache;

		m_mbr = Lazy.of(() -> {
			Envelope bounds = m_dsInfo.getBounds();
			return new ReferencedEnvelope(bounds, m_crs);
		});
	}
	
	public GSPFeatureSource setSampleCount(long count) {
		if ( count > 0 ) {
			m_sampleCount = Option.some(count);
		}
		else {
			m_sampleCount = Option.none();
		}
		
		return this;
	}
	
	
	public RecordSet query(Envelope range) {
		try {
			if ( range.contains(m_dsInfo.getBounds()) ) {
				return scan();
			}
			else if ( !m_dsInfo.hasSpatialIndex() ) {
				s_logger.info("no spatial index, use full scan: id={}", m_dsId);
				
				return scanRange(range, -1);
			}
			
			// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드를 계산한다.
			Tuple2<List<SpatialClusterInfo>, Long> result = guessRelevants(range);
			List<String> quadKeys = FStream.of(result._1)
											.map(SpatialClusterInfo::getQuadKey)
											.toList();
			long totalGuess = result._2;
			
			// 추정된 레결과 레코드 수를 통해 샘플링 비율을 계산한다.
			double sampleRatio = m_sampleCount.map(cnt -> (double)cnt / totalGuess)
											.getOrElse(1d);
			sampleRatio = sampleRatio > 1 ? 1 : sampleRatio;
			
			Envelope overlap = range.intersection(m_dsInfo.getBounds());
			double coverRatio = overlap.getArea() / m_dsInfo.getBounds().getArea();		
			if ( coverRatio > 0.7 ) {
				s_logger.info("too large for index, use full scan: id={}", m_dsId);
				
				return scanRange(range, sampleRatio);
			}
			
			// quad-key들 중에서 캐슁되지 않은 cluster들의 갯수를 구한다.
			List<List<Record>> cachedParts = FStream.of(quadKeys)
													.map(qkey -> m_cache.getIfPresent(m_dsId, qkey))
													.filter(part -> part != null)
													.toList();
			int nonCachedCount = quadKeys.size() - cachedParts.size();

			String ratioStr = String.format("%.02f%%", sampleRatio*100);
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("status: ds_id={}, nclusters={}, ncaches={}, guess_count={}, ratio={}",
								m_dsId, quadKeys.size(), cachedParts.size(), totalGuess, ratioStr);
			}
			
			if ( nonCachedCount > MAX_CACHING_CLUSTERS ) {
				return indexedScan(range, sampleRatio);
			}

			s_logger.info("querying with caches: ds_id={}, ratio={}", m_dsId, ratioStr);
			
			Geometry key = GeoClientUtils.toPolygon(range);
			PreparedGeometry pkey = PreparedGeometryFactory.prepare(key);
			FStream<Record> recStream = FStream.of(quadKeys)
												.flatMapIterable(qk -> m_cache.get(m_dsId, qk))
												.filter(r -> {
													Geometry geom = r.getGeometry(m_gcInfo.name());
													return pkey.intersects(geom);
												});
			if ( sampleRatio < 1 ) {
				recStream = recStream.sample(sampleRatio);
			}
			return RecordSets.from(m_dsInfo.getRecordSchema(), recStream);
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		try {
			if ( query == Query.ALL ) {
				return m_mbr.get();
			}
			else {
				Tuple2<BoundingBox, Option<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr.get(), query);
				if ( resolved._1 == null && resolved._2.isEmpty() ) {
					return new ReferencedEnvelope(m_mbr.get());
				}
				
				Plan plan = newPlanBuilder(resolved._1, resolved._2)
							.aggregate(AggregateFunction.ENVELOPE(m_gcInfo.name()))
							.build();
				Option<Envelope> envl = m_marmot.executeToSingle(plan);
				return envl.map(mbr -> new ReferencedEnvelope(mbr, m_crs))
							.getOrElse(() -> new ReferencedEnvelope());
			}
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected int getCountInternal(Query query) throws IOException {
		Tuple2<BoundingBox, Option<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr.get(), query);
		if ( resolved._1 == null && resolved._2.isEmpty() ) {
			return (int)m_dsInfo.getRecordCount();
		}
		
		Plan plan = newPlanBuilder(resolved._1, resolved._2)
						.aggregate(AggregateFunction.COUNT())
						.build();
		return m_marmot.executeToLong(plan).get().intValue();
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
		try {
			Tuple2<BoundingBox, Option<Filter>> resolved
											= GSPUtils.resolveQuery(m_mbr.get(), query);
			BoundingBox bbox = resolved._1;
			
			Envelope range = GeoClientUtils.toEnvelope(bbox.getMinX(), bbox.getMinY(),
														bbox.getMaxX(), bbox.getMaxY());
			RecordSet rset = query(range);
			
			return new GSPFeatureReader(getSchema(), new MarmotFeatureIterator(getSchema(), rset));
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	protected SimpleFeatureType buildFeatureType() throws IOException {
		return GeoToolsUtils.toSimpleFeatureType(getEntry().getTypeName(), m_gcInfo.srid(),
												m_dsInfo.getRecordSchema());
	}
	
	private PlanBuilder newPlanBuilder(BoundingBox bbox, Option<Filter> filter) {
		PlanBuilder builder = m_marmot.planBuilder("query_Dataset");
		
		if ( bbox != null ) {
			Geometry key = GeoClientUtils.toPolygon(bbox);
			builder.query(m_dsId, SpatialRelation.INTERSECTS, key);
		}
		else {
			builder.load(m_dsId);
		}
		
		return builder;
	}
	
	private Tuple2<List<SpatialClusterInfo>, Long> guessRelevants(Envelope range) {
		List<SpatialClusterInfo> infos = m_dsInfo.getDataSet()
												.querySpatialClusterInfo(range);
		
		List<Tuple2<SpatialClusterInfo,Long>> relevants = FStream.of(infos)
							.map(idx -> Tuple.of(idx, guessCount(range, idx)))
							.filter(t -> t._2 > 0)
							.toList();
		long total = FStream.of(relevants).mapToLong(r -> r._2).sum();
		List<SpatialClusterInfo> domain = FStream.of(relevants).map(r -> r._1).toList();
		
		return Tuple.of(domain, total);
	}
	
	private long guessCount(Envelope boundsWgs84, SpatialClusterInfo info) {
		Envelope bounds = info.getDataBounds().intersection(info.getTileBounds());
		double ratio =  boundsWgs84.intersection(bounds).getArea() / bounds.getArea();
		return Math.round(info.getRecordCount() * ratio);
	}
	
	private RecordSet indexedScan(Envelope range, double sampleRatio) {
		// 샘플 수가 정의되지 않거나, 대상 데이터세트의 레코드 갯수가 샘플 수보다 작은 경우
		// 데이터세트 전체를 반환한다. 성능을 위해 query() 연산 활용함.
		if ( sampleRatio >= 1 ) {
			s_logger.info("index_scan: no sampling");
			return m_dsInfo.getDataSet().queryRange(range, Option.none());
		}
		else {
			String ratioStr = String.format("%.2f%%", sampleRatio*100);
			s_logger.info("index_scan: ratio={}", ratioStr);
			
			String planName = String.format("index_scan(ratio=%.02f%%)", sampleRatio*100);
			Plan plan = m_marmot.planBuilder(planName)
								.query(m_dsId, INTERSECTS, range)
								.sample(sampleRatio)
								.build();
			return m_marmot.executeToRecordSet(plan);
		}
	}
	
	private RecordSet scan() {
		double ratio = m_sampleCount.map(cnt -> (double)cnt / m_dsInfo.getRecordCount())
									.getOrElse(1d);
		if ( ratio >= 1 ) {
			s_logger.info("scan fully: dataset={}", m_dsId);
			return m_dsInfo.getDataSet().read();
		}

		String ratioStr = String.format("%.2f%%", ratio*100);
		s_logger.info("scan fully: dataset={}, ratio={}", m_dsId, ratioStr);
		
		String planName = String.format("full_scan (sample=%s)", ratioStr);
		Plan plan = m_marmot.planBuilder(planName)
							.load(m_dsId)
							.sample(ratio)
							.take(m_sampleCount.get())
							.build();
		return m_marmot.executeToRecordSet(plan);
	}
	
	private RecordSet scanRange(Envelope range, double ratio) {
		if ( ratio >= 1 ) {
			s_logger.info("scan range: dataset={}", m_dsId);
			
			return m_dsInfo.getDataSet().queryRange(range, Option.none());
		}
		
		Plan plan;
		String dsId = (ratio >= 0) ? m_dsId : "tmp/" + UUID.randomUUID().toString();
		if ( ratio < 0 ) {
			s_logger.info("estimate sample ratio: dataset={}", m_dsId);
			
			// 샘플링 비율이 확정되지 않은 경우.
			// 필요한 샘플 수를 위한 샘플 ratio를 계산하기 위해 질의 조건을 만족하는
			// 데이터세트를 구한다.
			Geometry key = GeoClientUtils.toPolygon(range);
			GeometryColumnInfo gcInfo = m_dsInfo.getGeometryColumnInfo();
			plan = m_marmot.planBuilder("scan range")
							.query(m_dsId, INTERSECTS, key)
							.build();
			DataSet temp = m_marmot.createDataSet(dsId, plan, GEOMETRY(gcInfo), FORCE);
			ratio = (double)m_sampleCount.get() / temp.getRecordCount();
		}
		
		ratio = Math.min(ratio, 1);
		String ratioStr = String.format("%.2f%%", ratio*100);
		s_logger.info("scan over temporary dataset: ratio={}", ratioStr);

		String planName = String.format("scan over temporary (ratio=%.2f%%)", ratio*100);
		plan = m_marmot.planBuilder(planName)
						.load(dsId)
						.sample(ratio)
						.build();
		RecordSet result = m_marmot.executeLocally(plan);
		return RecordSets.attachCloser(result, () -> {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("purge temporary dataset: id={}", dsId);
			}
			m_marmot.deleteDataSet(dsId);
		});
	}
	
	private static class Sampler implements Predicate<Record> {
		private final float m_ratio;
		private final Random m_randGem = new Random(System.currentTimeMillis());
		
		Sampler(float ratio) {
			m_ratio = ratio;
		}

		@Override
		public boolean test(Record rec) {
			return m_randGem.nextFloat() <= m_ratio;
		}
	}
}