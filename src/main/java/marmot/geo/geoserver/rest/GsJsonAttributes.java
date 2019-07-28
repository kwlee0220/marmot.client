package marmot.geo.geoserver.rest;

import com.google.gson.annotations.SerializedName;

import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsJsonAttributes {
	@SerializedName("attribute") private GsJsonAttribute[] m_attr;
	
	private GsJsonAttributes(GsJsonAttribute[] attrs) {
		m_attr = attrs;
	}
	
	public static GsJsonAttributes from(RecordSchema schema) {
		return new GsJsonAttributes(schema.streamColumns()
											.map(GsJsonAttribute::new)
											.toArray(GsJsonAttribute.class));
	}
}
