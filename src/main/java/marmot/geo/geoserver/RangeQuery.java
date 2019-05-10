package marmot.geo.geoserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.RecordSet;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class RangeQuery {
	static final Logger s_logger = LoggerFactory.getLogger(RangeQuery.class);
	
	private final String m_dsId;
	private final DataSet m_ds;
	private final Envelope m_range;
	private FOption<Long> m_sampleCount = FOption.empty();
	private final int m_maxLocalCacheCost;
	private final DataSetPartitionCache m_cache;
	private volatile boolean m_usePrefetch = false;
	
	public static RangeQuery on(DataSet ds, Envelope range, DataSetPartitionCache cache,
								int maxLocalCacheCost) {
		return new RangeQuery(ds, range, cache, maxLocalCacheCost);
	}
	
	private RangeQuery(DataSet ds, Envelope range, DataSetPartitionCache cache,
						int maxLocalCacheCost) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query range");
		Utilities.checkNotNullArgument(cache, "DataSetPartitionCache");
		
		m_ds = ds;
		m_dsId = ds.getId();
		m_range = range;
		m_cache = cache;
		m_maxLocalCacheCost = maxLocalCacheCost;
	}
	
	public RangeQuery sampleCount(long count) {
		Preconditions.checkArgument(count > 0);
		
		m_sampleCount = FOption.of(count);
		return this;
	}
	
	public RangeQuery sampleCount(FOption<Long> count) {
		m_sampleCount = count;
		return this;
	}
	
	public RangeQuery usePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}
	
	public RecordSet run() {
		try {
			// 질의 영역이 DataSet 전체 영역보다 더 넓은 경우는 인덱스를 사용하는 방법보다
			// 그냥 full scan 방식을 사용한다.
			if ( m_range.contains(m_ds.getBounds()) ) {
				if ( m_sampleCount.isPresent() && m_ds.hasThumbnail() ) {
					s_logger.info("too large range, use thumbnail scan: id={}", m_dsId);
					return ThumbnailScan.scan(m_ds, m_range, m_sampleCount.get()).get();
				}
				else {
					s_logger.info("too large range, use full scan: id={}, nsamples={}",
									m_dsId, m_sampleCount);
					return FullScan.on(m_ds)
									.sampleCount(m_sampleCount)
									.run();
				}
			}
			
			// 대상 DataSet에 인덱스가 걸려있지 않는 경우에는 full scan 방식을 사용한다.
			if ( !m_ds.isSpatiallyClustered() ) {
				if ( m_ds.hasThumbnail() ) {
					s_logger.info("no spatial index, try to use mixed(thumbnail/full) scan: id={}", m_dsId);
					return m_sampleCount.flatMap(cnt -> ThumbnailScan.scan(m_ds, m_range, cnt))
										.getOrElse(() -> FullScan.on(m_ds)
																.range(m_range)
																.sampleCount(m_sampleCount)
																.run());
				}
				else {
					return FullScan.on(m_ds).range(m_range).sampleCount(m_sampleCount).run();
				}
			}
			else {
				// 질의 영역과 겹치는 quad-key들과, 해당 결과 레코드의 수를 추정한다.
				return IndexScan.on(m_ds, m_range, m_cache, m_maxLocalCacheCost)
								.sampleCount(m_sampleCount)
								.usePrefetch(m_usePrefetch)
								.run();
			}
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}
}
