package marmot.geo.geoserver.rest;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsJsonExclusionStrategy implements ExclusionStrategy {

	@Override
	public boolean shouldSkipField(FieldAttributes field) {
		return field.getAnnotation(Skip.class) != null;
	}

	@Override
	public boolean shouldSkipClass(Class<?> clazz) {
		return clazz.getAnnotation(Skip.class) != null;
	}
}
