package marmot.geo.geoserver.rest;

import com.google.gson.annotations.SerializedName;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsJsonFeatureTypeInfo {
	@SerializedName("featureType") private GsJsonFeatureType m_featureType;
	
	public GsJsonFeatureTypeInfo(GsJsonFeatureType ft) {
		m_featureType = ft;
	}
}
