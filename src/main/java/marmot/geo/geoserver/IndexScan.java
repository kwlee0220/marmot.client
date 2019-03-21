package marmot.geo.geoserver;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.support.RangedClusterEstimate;
import utils.StopWatch;
import utils.async.AbstractThreadedExecution;
import utils.async.StartableExecution;
import utils.async.op.AsyncExecutions;
import utils.func.FOption;
import utils.stream.AdaptableSamplingStream;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class IndexScan {
	private static final Logger s_logger = LoggerFactory.getLogger(IndexScan.class);
	
	private static final int MAX_CACHE_SIZE = 10;
	private static final int MAX_DISK_IO = 7;
	private static final int MAX_NETWORK_IO = 4;
	private static final int TOO_MUCH_CLUSTERS = 12;
	
	private final MarmotRuntime m_marmot;
	private final DataSet m_ds;
	private final String m_dsId;
	private final RangedClusterEstimate m_est;
	private final Envelope m_range;
	private final PreparedGeometry m_pkey;
	private FOption<Long> m_sampleCount = FOption.empty();
	private double m_sampleRatio;
	private final DataSetPartitionCache m_cache;
	private volatile boolean m_usePrefetch = false;
	
	static IndexScan on(DataSet ds, Envelope range, DataSetPartitionCache cache) {
		return new IndexScan(ds, range, cache);
	}
	
	private IndexScan(DataSet ds, Envelope range, DataSetPartitionCache cache) {
		Objects.requireNonNull(ds, "DataSet");
		Objects.requireNonNull(range, "query ranage");
		
		m_marmot = ds.getMarmotRuntime();
		m_ds = ds;
		m_dsId = m_ds.getId();
		m_range = range;
		m_cache = cache;
		
		Geometry key = GeoClientUtils.toPolygon(m_range);
		m_pkey = PreparedGeometryFactory.prepare(key);

		// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드의 수를 계산한다.
		m_est = RangedClusterEstimate.about(m_ds, m_range);
	}
	
	public IndexScan sampleCount(FOption<Long> count) {
		m_sampleCount = count;
		return this;
	}
	
	public IndexScan usePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}
	
	public RecordSet run() {
		// 추정된 레결과 레코드 수를 통해 샘플링 비율을 계산한다.
		double sampleRatio = m_sampleCount.map(cnt -> (double)cnt / m_est.getTotalMatchCount())
											.getOrElse(1d);
		m_sampleRatio = sampleRatio > 1 ? 1 : sampleRatio;
		
		double fullRatio = (m_sampleRatio * m_est.getTotalMatchCount()) / m_ds.getRecordCount();
		if ( fullRatio > 0.7 ) {
			s_logger.info("too large for index-scan, use mixed-scan: id={}", m_dsId);
			
			return m_sampleCount.flatMap(cnt -> ThumbnailScan.scan(m_ds, m_range, cnt))
								.getOrElse(() -> FullScan.on(m_ds)
														.range(m_range)
														.sampleRatio(m_sampleRatio)
														.run());
		}
		
		//
		// use index-scan with local cache from now on...
		//
		
		// quad-key들 중에서 캐슁되지 않은 cluster들의 갯수를 구한다.
		int nclusters = m_est.getRelevantClusterCount();
		List<String> cachedKeys = FStream.from(m_est.getRelevantQuadKeys())
										.filter(qkey -> m_cache.existsAtDisk(m_dsId, qkey))
										.toList();
		List<String> memCachedKeys = FStream.from(cachedKeys)
											.filter(qkey -> m_cache.existsAtMemory(m_dsId, qkey))
											.toList();
		int nonCachedCount = nclusters - cachedKeys.size();
		int diskIOCount = cachedKeys.size() - memCachedKeys.size();

		String ratioStr = String.format("%.02f%%", m_sampleRatio*100);
		s_logger.info("status: ds_id={}, cache={}:{}:{} guess_count={}, ratio={}",
						m_dsId, memCachedKeys.size(), diskIOCount, nonCachedCount,
						m_est.getTotalMatchCount(), ratioStr);
		
		if ( nclusters > MAX_CACHE_SIZE || diskIOCount > MAX_DISK_IO
			|| nonCachedCount > MAX_NETWORK_IO ) {
			if ( m_usePrefetch ) {
				StartableExecution<RecordSet> fg = AsyncExecutions.from(() -> runAtServer(nclusters));
				StartableExecution<?> bg = forkClusterPrefetcher();
				StartableExecution<RecordSet> exec = AsyncExecutions.backgrounded(fg, bg);
				exec.start();
				
				return exec.getUnchecked();
			}
			else {
				return runAtServer(nclusters);
			}
		}
		else {
			return runOnLocalCache();
		}
	}
	
	private RecordSet runAtServer(int nclusters) {
		if ( m_ds.hasThumbnail() && m_sampleCount.isPresent()
			&& nclusters >= TOO_MUCH_CLUSTERS ) {
			s_logger.info("try to use thumbnail-scan: ds_id={}, nclusters={}",
							m_dsId, nclusters);
			FOption<RecordSet> orset = ThumbnailScan.scan(m_ds, m_range, m_sampleCount.get());
			if ( orset.isPresent() ) {
				return orset.get();
			}
		}
		
		String ratioStr = String.format("%.2f%%", m_sampleRatio*100);
		s_logger.info("use a normal index-scan: ds_id={}, ratio={}", m_dsId, ratioStr);

		String planName = String.format("index_scan(ratio=%s)", ratioStr);
		
		// 샘플 수가 정의되지 않거나, 대상 데이터세트의 레코드 갯수가 샘플 수보다 작은 경우
		// 데이터세트 전체를 반환한다. 성능을 위해 query() 연산 활용함.
		if ( m_sampleRatio >= 1 ) {
			Plan plan = m_marmot.planBuilder(planName)
								.query(m_dsId, INTERSECTS, m_range)
								.take(m_sampleCount.get())
								.build();
			return m_marmot.executeLocally(plan);
		}
		else {
			Plan plan = m_marmot.planBuilder(planName)
								.query(m_dsId, INTERSECTS, m_range)
								.sample(m_sampleRatio)
								.take(m_sampleCount.get())
								.build();
			return m_marmot.executeToRecordSet(plan);
		}
	}
	
	private RecordSet runOnLocalCache() {
		String ratioStr = String.format("%.2f%%", m_sampleRatio*100);
		s_logger.info("query on caches: ds_id={}, ratio={}", m_dsId, ratioStr);
		
		FStream<Record> recStream = FStream.from(m_est.getRelevantQuadKeys())
											.flatMap(qk -> readFromCache(qk));
		
		return RecordSet.from(m_ds.getRecordSchema(), recStream);
	}
	
	private FStream<Record> readFromCache(String quadKey) {
		String geomColName = m_ds.getGeometryColumn();
		FStream<Record> matcheds = FStream.from(m_cache.get(m_dsId, quadKey))
											.filter(r -> {
												Geometry geom = r.getGeometry(geomColName);
												return m_pkey.intersects(geom);
											});
		if ( m_sampleRatio < 1 ) {
			// quadKey에 해당하는 파티션에 샘플링할 레코드 수를 계산하고
			// 이 수만큼의 레코드만 추출하도록 연산을 추가한다.
			long total = m_est.getRelevantRecordCount(quadKey);
			matcheds = new AdaptableSamplingStream<>(matcheds, total, m_sampleRatio);
		}
		
		return matcheds;
	}
	
	private StartableExecution<Void> forkClusterPrefetcher() {
		FStream<StartableExecution<?>> strm = getNextNonCachedQuadKey().fstream()
												.map(Prefetcher::new);
		return AsyncExecutions.sequential(strm);
	}
	
	private FOption<String> getNextNonCachedQuadKey() {
		List<String> keys = FStream.from(m_est.getRelevantQuadKeys())
									.filter(qkey -> !m_cache.existsAtDisk(m_dsId, qkey))
									.max((k1,k2) -> k1.length() - k2.length());
		return keys.isEmpty() ? FOption.empty() : FOption.of(keys.get(0));
	}
	
	private class Prefetcher extends AbstractThreadedExecution<Void> {
		private final String m_quadKey;
		
		Prefetcher(String quadKey) {
			m_quadKey = quadKey;
		}
		
		@Override
		protected Void executeWork() throws Exception {
			StopWatch watch = StopWatch.start();
			InputStream cluster = m_ds.readRawSpatialCluster(m_quadKey);
			m_cache.put(m_dsId, m_quadKey, cluster);
			
			s_logger.debug("prefetched: ds={}, quadkey={}, elapsed={}",
							m_dsId, m_quadKey, watch.stopAndGetElpasedTimeString());
			return null;
		}
		
		@Override
		public String toString() {
			return String.format("PartitionPrefetcher[%s]", m_quadKey);
		}
	}
}