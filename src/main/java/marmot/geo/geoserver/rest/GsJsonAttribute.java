package marmot.geo.geoserver.rest;

import com.google.gson.annotations.SerializedName;

import marmot.Column;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsJsonAttribute {
	@SerializedName("name") private final String m_name;
	@SerializedName("minOccurs") private final int m_minOccurs;
	@SerializedName("maxOccurs") private final int m_maxOccurs;
	@SerializedName("nillable") private final boolean m_nillable;
	@SerializedName("binding") private final String m_binding;
	
	public GsJsonAttribute(Column col) {
		m_name = col.name();
		m_minOccurs = 0;
		m_maxOccurs = 1;
		m_nillable = true;
		m_binding = col.type().getInstanceClass().getName();
	}
}
