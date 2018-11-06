package marmot.geoserver.plugin;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

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
import utils.Throwables;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPFeatureSource extends ContentFeatureSource {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPFeatureSource.class);

	private final MarmotRuntime m_marmot;
	private final CachingDataSet m_ds;
	private final GeometryColumnInfo m_gcInfo;
	private final ReferencedEnvelope m_mbr;
	private final CoordinateReferenceSystem m_crs;
	private Option<Long> m_sampleCount = Option.none();
	
	GSPFeatureSource(ContentEntry entry, CachingDataSet ds) {
		super(entry, Query.ALL);
		
		m_marmot = ds.getMarmotRuntime();
		m_ds = ds;		
		m_gcInfo = ds.getGeometryColumnInfo();
		m_crs = CRSUtils.toCRS(m_gcInfo.srid());

		Envelope bounds = ds.getBounds();
		m_mbr = new ReferencedEnvelope(bounds, m_crs);
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

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		try {
			if ( query == Query.ALL ) {
				return m_mbr;
			}
			else {
				Tuple2<BoundingBox, Option<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr, query);
				if ( resolved._1 == null && resolved._2.isEmpty() ) {
					return new ReferencedEnvelope(m_mbr);
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
										= GSPUtils.resolveQuery(m_mbr, query);
		if ( resolved._1 == null && resolved._2.isEmpty() ) {
			return (int)m_ds.getRecordCount();
		}
		
		Plan plan = newPlanBuilder(resolved._1, resolved._2)
						.aggregate(AggregateFunction.COUNT())
						.build();
		return m_marmot.executeToLong(plan).get().intValue();
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
		try {
			Tuple2<BoundingBox, Option<Filter>> resolved = GSPUtils.resolveQuery(m_mbr, query);
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
												m_ds.getRecordSchema());
	}
	
	private PlanBuilder newPlanBuilder(BoundingBox bbox, Option<Filter> filter) {
		PlanBuilder builder = m_marmot.planBuilder("query_Dataset");
		
		if ( bbox != null ) {
			Geometry key = GeoClientUtils.toPolygon(bbox);
			builder.query(m_ds.getId(), SpatialRelation.INTERSECTS, key);
		}
		else {
			builder.load(m_ds.getId());
		}
		
		return builder;
	}
	
	private Tuple2<List<SpatialClusterInfo>, Long> guessRelevants(Envelope range) {
		List<SpatialClusterInfo> infos = m_ds.querySpatialClusterInfo(range);
		
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
	
	private RecordSet sample(List<String> quadKeys, Envelope range, double ratio) {
		// 읽어야 하는 클러스터들 중에서 현재 caching되어 있지 않은 클러스터의 수를 계산함.
		long cacheCount = FStream.of(quadKeys)
									.filter(qkey -> m_ds.isCached(qkey))
									.count();
		String ratioStr = String.format("%.02f%%", ratio * 100);
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("status: ds_id={}, total={}, cached={}, sample={}({})",
						m_ds.getId(), quadKeys.size(), cacheCount,
						m_sampleCount.get(), ratioStr);
		}
		
		String dsId = m_ds.getId();
		if ( range.contains(m_ds.getBounds()) ) {
			// 질의 영역에 대상 데이터세트의 전체 영역보다 큰 경우는 클러스터를 활용한 데이터 접근을 사용하지 않고,
			// 원시  데이터세트를 접근하여 질의 결과를 얻는다.
			s_logger.info("sampling from raw dataset: dsid={}, sample={}",
							dsId, m_sampleCount.get());

			String planId = String.format("sample_%s_%s", dsId, ratioStr);
			Plan plan = m_marmot.planBuilder(planId)
								.load(dsId)
								.sample(ratio)
								.build();
			return m_marmot.executeToRecordSet(plan);
		}
		else {
			String planName = String.format("sample[%s, ratio=%s]", dsId, ratioStr);
			Geometry key = GeoClientUtils.toPolygon(range);
			Plan plan = m_marmot.planBuilder(planName)
								.query(dsId, SpatialRelation.INTERSECTS, key)
								.sample(ratio)
								.build();
			return m_marmot.executeToRecordSet(plan);
		}
	}
	
	public RecordSet query(Envelope range) {
		try {
			if ( m_ds.getDefaultSpatialIndexInfoOrNull() == null ) {
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("no spatial index, use full scan: id={}", m_ds.getId());
				}
				
				return scan(range);
			}
			
			Tuple2<List<SpatialClusterInfo>, Long> result = guessRelevants(range);
			List<String> quadKeys = FStream.of(result._1)
											.map(SpatialClusterInfo::getQuadKey)
											.toList();
			long totalGuess = result._2;
			double sampleRatio = m_sampleCount.map(cnt -> (double)cnt / totalGuess)
												.getOrElse(1d);
			List<String> nonCacheds = FStream.of(quadKeys).filter(qkey -> !m_ds.isCached(qkey)).toList();
			int ncaches = quadKeys.size() - nonCacheds.size();
			boolean sampling = m_sampleCount.isDefined() && nonCacheds.size() > 3;
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("status: ds_id={}, nclusters={}, ncaches={}, guess_count={} -> {}",
								m_ds.getId(), quadKeys.size(), ncaches, totalGuess,
								sampling ? "sample range" : "caching range");
			}
			
			RecordSet rset;
			if ( sampling ) {
				rset = sample(quadKeys, range, sampleRatio);
			}
			else {
				FStream<Record> recStream = FStream.of(quadKeys)
													.flatMap(qk -> query(qk, range, sampleRatio));
				rset = RecordSets.from(recStream);
			}
			
			return rset;
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}
	
	private FStream<Record> query(String quadKey, Envelope range, double ratio) {
		return m_ds.readCluster(quadKey)
					.fstream()
					.filter(r -> {
						Geometry geom = r.getGeometry(m_gcInfo.name());
						Envelope mbr = geom.getEnvelopeInternal();
						return range.intersects(mbr);
					})
					.sample(ratio);
	}
	
	private RecordSet scan(Envelope range) {
		// 샘플 수가 정의되지 않거나, 대상 데이터세트의 레코드 갯수가 샘플 수보다 작은 경우
		// 데이터세트 전체를 반환한다. 성능을 위해 query() 연산 활용함.
		if ( m_sampleCount.isEmpty() || m_ds.getRecordCount() <= m_sampleCount.get() ) {
			return m_ds.query(range, Option.none());
		}
		
		// 필요한 샘플 수를 위한 샘플 ratio를 계산하기 위해 질의 조건을 만족하는
		// 데이터세트를 구한다.
		GeometryColumnInfo gcInfo = m_ds.getGeometryColumnInfo();
		final String tempId = "tmp/" + UUID.randomUUID().toString();
		Plan plan = m_marmot.planBuilder("query")
							.query(m_ds.getId(), SpatialRelation.INTERSECTS, range)
							.build();
		DataSet temp = m_marmot.createDataSet(tempId, plan, GEOMETRY(gcInfo), FORCE);
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("create temporary dataset: id={}", tempId);
		}
		
		double ratio = (double)m_sampleCount.get() / temp.getRecordCount();
		plan = m_marmot.planBuilder("sampling")
						.load(tempId)
						.sample(ratio)
						.build();
		RecordSet result = m_marmot.executeLocally(plan);
		return RecordSets.attachCloser(result, () -> {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("purge temporary dataset: id={}", tempId);
			}
			m_marmot.deleteDataSet(tempId);
		});
	}
}