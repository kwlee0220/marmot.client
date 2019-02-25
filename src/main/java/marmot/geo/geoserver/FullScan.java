package marmot.geo.geoserver;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.rset.RecordSets;
import utils.LoggerSettable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FullScan implements LoggerSettable {
	private final MarmotRuntime m_marmot;
	private final DataSet m_ds;
	@Nullable private Envelope m_range;
	private double m_sampleRatio = -1;
	private FOption<Long> m_sampleCount = FOption.empty();
	private FOption<DataSet> m_rangedDs = FOption.empty();
	private Logger m_logger;
	
	static FullScan on(DataSet ds) {
		return new FullScan(ds);
	}
	
	private FullScan(DataSet ds) {
		Objects.requireNonNull(ds, "DataSet");
		
		m_marmot = ds.getMarmotRuntime();
		m_ds = ds;
		
		m_logger = LoggerFactory.getLogger(FullScan.class);
	}
	
	public FullScan range(Envelope range) {
		m_range = range;
		return this;
	}
	
	public FullScan sampleRatio(double ratio) {
		m_sampleRatio = ratio;
		return this;
	}
	
	public FullScan sampleCount(long count) {
		Preconditions.checkArgument(count > 0);
		
		m_sampleCount = FOption.of(count);
		return this;
	}
	
	public FullScan sampleCount(FOption<Long> count) {
		m_sampleCount = count;
		return this;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	public RecordSet run() {
		double ratio = getSampleRatio();
		String dsId = m_rangedDs.map(DataSet::getId)
								.getOrElse(m_ds.getId());
		
		String msg = String.format("full-scan: dataset=%s, ratio=%.2f%%",
									dsId, ratio*100);
		getLogger().info(msg);

		PlanBuilder builder = m_marmot.planBuilder(msg);
		if ( m_rangedDs.isPresent() ) {
			builder = builder.load(dsId);
		}
		else if ( m_range != null ) {
			Geometry key = GeoClientUtils.toPolygon(m_range);
			builder = builder.query(dsId, INTERSECTS, key);
		}
		else {
			builder = builder.load(dsId);
		}
		if ( ratio < 1 ) {
			builder = builder.sample(ratio);
			builder = m_sampleCount.transform(builder, (b,c) -> b.take(c));
		}
		Plan plan = builder.build();
		
		RecordSet result = m_marmot.executeLocally(plan);
		if ( m_rangedDs.isPresent() ) {
			result = RecordSets.attachCloser(result, () -> {
									getLogger().info("purge temporary dataset: id={}", dsId);
									m_marmot.deleteDataSet(dsId);
								});
		}
		return result;
	}
	
	private double getSampleRatio() {
		if ( m_sampleRatio > 0 ) {
			return m_sampleRatio;
		}
		else if ( m_sampleCount.isAbsent() ) {
			return 1d;
		}
		
		long count = m_sampleCount.get();
		if ( m_range == null ) {
				// 샘플 갯수를 이용하여 샘플링 비율을 추정한다.
			return (double)count / m_ds.getRecordCount();
		}
		else {
			// 먼저 질의 영역에 속한 레코드를 질의하고, 이를 바탕으로 샘플링 비율을 계산함
			// 이때, 구한 데이터 세트는 샘플링할 때도 사용한다.
			String rangedDsId = "tmp/" + UUID.randomUUID().toString();
			
			Geometry key = GeoClientUtils.toPolygon(m_range);
			GeometryColumnInfo gcInfo = m_ds.getGeometryColumnInfo();
			
			Plan plan;
			plan = m_marmot.planBuilder("scan range")
							.query(m_ds.getId(), INTERSECTS, key)
							.build();
			DataSet result = m_marmot.createDataSet(rangedDsId, plan, GEOMETRY(gcInfo), FORCE);
			m_rangedDs = FOption.of(result);
			return (double)count / result.getRecordCount();
		}
	}
}
