package marmot.geo.geoserver;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Throwables;
import utils.Tuple;
import utils.func.FOption;
import utils.func.Lazy;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CRSUtils;
import marmot.geo.GeoClientUtils;
import marmot.geo.geotools.MarmotFeatureIterator;
import marmot.geo.geotools.SimpleFeatures;
import marmot.geo.query.GeoDataStore;
import marmot.optor.AggregateFunction;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPFeatureSource extends ContentFeatureSource {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPFeatureSource.class);
	
	private final MarmotRuntime m_marmot;
	private final GeoDataStore m_store;
	private final String m_dsId;
	private final DataSet m_ds;
	private final GeometryColumnInfo m_gcInfo;
	private final CoordinateReferenceSystem m_crs;
	private final Lazy<ReferencedEnvelope> m_mbr;
	
	GSPFeatureSource(ContentEntry entry, GeoDataStore store, String dsId) {
		super(entry, Query.ALL);
		
		m_marmot = store.getMarmotRuntime();
		m_store = store;
		m_dsId = dsId;
		m_ds = store.getGeoDataSet(dsId);

		m_gcInfo = m_ds.getGeometryColumnInfo();
		m_crs = CRSUtils.toCRS(m_gcInfo.srid());
		m_mbr = Lazy.of(() -> {
			Envelope bounds = m_ds.getBounds();
			return new ReferencedEnvelope(bounds, m_crs);
		});
	}
	
	public RecordSet query(Envelope range) throws Exception {
		return m_store.createRangeQuery(m_dsId, range).run();
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		try {
			if ( query == Query.ALL ) {
				return m_mbr.get();
			}
			else {
				Tuple<BoundingBox, FOption<Filter>> resolved
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
		Tuple<BoundingBox, FOption<Filter>> resolved
										= GSPUtils.resolveQuery(m_mbr.get(), query);
		if ( resolved._1 == null && resolved._2.isAbsent() ) {
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
			Tuple<BoundingBox, FOption<Filter>> resolved
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
													m_ds.getRecordSchema());
	}
	
	private PlanBuilder newPlanBuilder(BoundingBox bbox, FOption<Filter> filter) {
		PlanBuilder builder = Plan.builder("query_Dataset");
		
		if ( bbox != null ) {
			builder = builder.query(m_dsId, GeoClientUtils.toEnvelope(bbox));
		}
		else {
			builder.load(m_dsId);
		}
		
		return builder;
	}
}