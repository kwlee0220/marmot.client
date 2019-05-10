package marmot.geo.geoserver;

import java.io.IOException;

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

import io.vavr.Lazy;
import io.vavr.Tuple2;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.geo.CRSUtils;
import marmot.geo.GeoClientUtils;
import marmot.geo.geotools.MarmotFeatureIterator;
import marmot.geo.geotools.SimpleFeatures;
import marmot.optor.AggregateFunction;
import utils.Throwables;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPFeatureSource extends ContentFeatureSource {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPFeatureSource.class);
	
	private final MarmotRuntime m_marmot;
	private final GSPDataSetInfo m_dsInfo;
	private final String m_dsId;
	private final DataSetPartitionCache m_cache;
	private final GeometryColumnInfo m_gcInfo;
	private final Lazy<ReferencedEnvelope> m_mbr;
	private final CoordinateReferenceSystem m_crs;
	private FOption<Long> m_sampleCount = FOption.empty();
	private volatile boolean m_usePrefetch = false;
	private final int m_maxLocalCacheCost;
	
	GSPFeatureSource(ContentEntry entry, GSPDataSetInfo info, DataSetPartitionCache cache,
					int maxLocalCacheCost) {
		super(entry, Query.ALL);
		
		m_marmot = info.getMarmotRuntime();
		m_dsInfo = info;
		m_dsId = m_dsInfo.getDataSet().getId();
		m_gcInfo = m_dsInfo.getDataSet().getGeometryColumnInfo();
		m_crs = CRSUtils.toCRS(m_gcInfo.srid());
		m_cache = cache;
		m_maxLocalCacheCost = maxLocalCacheCost;

		m_mbr = Lazy.of(() -> {
			Envelope bounds = m_dsInfo.getBounds();
			return new ReferencedEnvelope(bounds, m_crs);
		});
	}
	
	public GSPFeatureSource setSampleCount(FOption<Long> count) {
		m_sampleCount = count;
		return this;
	}
	
	public GSPFeatureSource usePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}
	
	public RecordSet query(Envelope range) {
		return RangeQuery.on(m_dsInfo.getDataSet(), range, m_cache, m_maxLocalCacheCost)
						.sampleCount(m_sampleCount)
						.usePrefetch(m_usePrefetch)
						.run();
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		try {
			if ( query == Query.ALL ) {
				return m_mbr.get();
			}
			else {
				Tuple2<BoundingBox, FOption<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr.get(), query);
				if ( resolved._1 == null && resolved._2.isAbsent() ) {
					return new ReferencedEnvelope(m_mbr.get());
				}
				
				Plan plan = newPlanBuilder(resolved._1, resolved._2)
							.aggregate(AggregateFunction.ENVELOPE(m_gcInfo.name()))
							.build();
				FOption<Envelope> envl = m_marmot.executeToSingle(plan);
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
		Tuple2<BoundingBox, FOption<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr.get(), query);
		if ( resolved._1 == null && resolved._2.isAbsent() ) {
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
			Tuple2<BoundingBox, FOption<Filter>> resolved
											= GSPUtils.resolveQuery(m_mbr.get(), query);
			BoundingBox bbox = resolved._1;
			Envelope range = GeoClientUtils.toEnvelope(bbox.getMinX(), bbox.getMinY(),
														bbox.getMaxX(), bbox.getMaxY());
			RecordSet rset = query(range);
			return new GSPFeatureReader(getSchema(), new MarmotFeatureIterator(getSchema(),
										rset));
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	protected SimpleFeatureType buildFeatureType() throws IOException {
		return SimpleFeatures.toSimpleFeatureType(getEntry().getTypeName(), m_gcInfo.srid(),
												m_dsInfo.getRecordSchema());
	}
	
	private PlanBuilder newPlanBuilder(BoundingBox bbox, FOption<Filter> filter) {
		PlanBuilder builder = m_marmot.planBuilder("query_Dataset");
		
		if ( bbox != null ) {
			Geometry key = GeoClientUtils.toPolygon(bbox);
			builder.query(m_dsId, key);
		}
		else {
			builder.load(m_dsId);
		}
		
		return builder;
	}
}