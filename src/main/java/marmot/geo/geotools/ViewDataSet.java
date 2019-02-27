package marmot.geo.geotools;

import java.io.IOException;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.map.DefaultMapContext;
import org.geotools.map.MapContext;
import org.geotools.swt.SwtMapFrame;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import utils.CSV;
import utils.CommandLine;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ViewDataSet {
	public static final void run(MarmotRuntime marmot, CommandLine cl) throws IOException {
		MapContext context = new DefaultMapContext();
	    context.setTitle("Marmot DataSet Viewer");
		
	    String srid = null;
	    SimpleFeatureCollection sfColl;
		for ( String dsId: cl.getArgumentAll() ) {
			List<String> parts = CSV.parseCsv(dsId, ':', '\\').toList();
			
			DataSet ds = marmot.getDataSet(parts.get(0));
			GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
			
			long sampleCount = -1;
			if ( parts.size() >= 2 ) {
				sampleCount = Long.parseLong(parts.get(1));
			}
			
			if ( srid == null ) {
				srid = gcInfo.srid();
			}
			if ( !srid.equals(gcInfo.srid()) ) {
				sfColl = (sampleCount > 0) ? sample(ds, gcInfo.srid(), sampleCount)
											: read(ds, gcInfo.srid());
			}
			else {
				sfColl = (sampleCount > 0) ? sample(ds, sampleCount) : read(ds);
			}
			
		    context.addLayer(sfColl, null);
		}
		
	    // and show the map viewer
	    SwtMapFrame.showMap(context);
	}
	
	private static SimpleFeatureCollection read(DataSet ds, String tarSrid) {
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder()
						.load(ds.getId())
						.transformCrs(gcInfo.name(), gcInfo.srid(), tarSrid)
						.build();
		MarmotRuntime marmot = ds.getMarmotRuntime();
		return SimpleFeatures.toFeatureCollection(ds.getId(), marmot, plan,
												ds.getGeometryColumnInfo().srid());
	}
	
	private static SimpleFeatureCollection sample(DataSet ds, String tarSrid, long count) {
		double ratio = (double)count / ds.getRecordCount();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder()
						.load(ds.getId())
						.sample(ratio)
						.transformCrs(gcInfo.name(), gcInfo.srid(), tarSrid)
						.build();
		MarmotRuntime marmot = ds.getMarmotRuntime();
		return SimpleFeatures.toFeatureCollection(ds.getId(), marmot, plan,
												ds.getGeometryColumnInfo().srid());
	}
	
	private static SimpleFeatureCollection read(DataSet ds) {
		return SimpleFeatures.toFeatureCollection(ds.getId(), ds);
	}
	
	private static SimpleFeatureCollection sample(DataSet ds, long count) {
		double ratio = (double)count / ds.getRecordCount();
		Plan plan = Plan.builder()
						.load(ds.getId())
						.sample(ratio)
						.build();
		MarmotRuntime marmot = ds.getMarmotRuntime();
		return SimpleFeatures.toFeatureCollection(ds.getId(), marmot, plan,
												ds.getGeometryColumnInfo().srid());
	}
}
