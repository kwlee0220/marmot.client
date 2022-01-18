package marmot.geo.geoserver;

import java.util.List;

import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.util.factory.GeoTools;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPUtils {
	private GSPUtils() {
		throw new AssertionError("Should not be called: class=" + GSPUtils.class);
	}
	
	public static String toSimpleFeatureTypeName(String dsId) {
		if ( dsId.charAt(0) == '/' ) {
			dsId = dsId.substring(1);
		}
		
		return dsId.replace('/', '.');
	}
	
	public static String toDataSetId(String sfTypeName) {
		return "/" + sfTypeName.replace('.', '/');
	}
	
	static Tuple<BoundingBox,FOption<Filter>> resolveQuery(BoundingBox mbr, Query query) {
		Tuple<FOption<BoundingBox>,FOption<Filter>> ret = parseCql(query.getFilter());
		
		if ( ret._1.isPresent() ) {
			BoundingBox bbox = ret._1.get();
//			if ( bbox.contains(mbr) ) {
//				return Tuple.of(null, ret._2);
//			}
//			else {
				return Tuple.of(bbox, ret._2);
//			}
		}
		else {
			return Tuple.of(null, ret._2);
		}
	}

	private static final FilterFactory2 FILTER_FACT
					= CommonFactoryFinder.getFilterFactory2(GeoTools.getDefaultHints());
	private static Tuple<FOption<BoundingBox>,FOption<Filter>> parseCql(Filter filter) {
		Utilities.checkNotNullArgument(filter, "filter is null");

		if ( filter instanceof BBOX ) {
			BoundingBox bbox = ((BBOX)filter).getBounds();
			return Tuple.of(FOption.of(bbox), FOption.empty());
		}
		else if ( filter instanceof And ) {
			List<Filter> filters = ((And)filter).getChildren();
			FOption<BBOX> bbox = FStream.from(filters)
										.castSafely(BBOX.class)
										.next();
			if ( bbox.isPresent() ) {
				FOption<BoundingBox> bounds = bbox.map(BBOX::getBounds);
				filter = FILTER_FACT.and(FStream.from(filters)
												.filter(f -> f != bbox.get())
												.toList());
				
				return Tuple.of(bounds, FOption.of(filter));
			}
			else {
				return Tuple.of(FOption.empty(), FOption.of(filter));
			}
		}
		else {
			return Tuple.of(FOption.empty(), FOption.of(filter));
		}
	}
}
