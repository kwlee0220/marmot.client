package marmot.geo.geoserver.rest;

import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.gson.annotations.SerializedName;
import com.vividsolutions.jts.geom.Envelope;

import marmot.Column;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.RecordSchema;
import marmot.geo.CRSUtils;
import marmot.geo.CoordinateTransform;
import marmot.geo.geoserver.GSPUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsJsonFeatureType {
	@Skip private String m_urlPrefix;
	@Skip private String m_nsUrl;
	@SerializedName("name") private String m_name;
	@SerializedName("nativeName") private String m_nativeName;
	@SerializedName("namespace") private GsJsonNameSpace m_ns;
	@SerializedName("title") private String m_title;
	@SerializedName("keywords") private GsJsonKeywords m_keywords;
	@SerializedName("nativeCRS") private GsJsonNativeCRS m_nativeCrs;
	@SerializedName("srs") private String m_srs;
	@SerializedName("nativeBoundingBox") private GsJsonBoundingBox m_nativeBoundingBox;
	@SerializedName("latLonBoundingBox") private GsJsonBoundingBox m_latLonBoundingBox;
	@SerializedName("projectionPolicy") private String m_projectionPolicy = "FORCE_DECLARED";
	@SerializedName("enabled") private boolean m_enabled = true;
	@SerializedName("store") private final GsJsonStore m_store;
	@SerializedName("maxFeatures") private final long m_maxFeatures;
	@SerializedName("numDecimals") private final int m_numDecimals = 0;
	@SerializedName("skipNumberMatched") private boolean m_skipNumberMatched = false;
	@SerializedName("circularArcPresent") private boolean m_circularArcPresent = false;
	@SerializedName("attributes") private GsJsonAttributes m_attrs;
	
	public GsJsonFeatureType(String url, String workspace, String storeName, DataSet ds) {
		m_urlPrefix = url;
		m_nsUrl = String.format("%s/namespaces/%s.json", url, workspace);
		m_name = m_nativeName = m_title = GSPUtils.toSimpleFeatureTypeName(ds.getId());
		m_keywords = new GsJsonKeywords(m_name);
		m_ns = new GsJsonNameSpace(workspace, m_nsUrl);
		
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		m_srs = gcInfo.srid();
		CoordinateReferenceSystem crs = CRSUtils.toCRS(m_srs);
		m_nativeCrs = new GsJsonNativeCRS(crs);
		
		Envelope envl = ds.getBounds();
		m_nativeBoundingBox = new GsJsonBoundingBox(envl, m_srs);
		
		CoordinateTransform trans = CoordinateTransform.get(m_srs, "EPSG:4326");
		m_latLonBoundingBox = new GsJsonBoundingBox(trans.transform(envl), "EPSG:4326");
		
		m_store = new GsJsonStore(workspace, storeName, url);
		m_maxFeatures = ds.getRecordCount();
		
		m_attrs = GsJsonAttributes.from(ds.getRecordSchema());
	}
	
	public String getName() {
		return m_name;
	}
	
	public String getUrl() {
		return String.format("%s/workspaces/%s/datastores/%s/featuretypes/%s.json",
							m_urlPrefix, m_ns.m_name, m_store.m_id, m_name);
	}
	
	private static class GsJsonNameSpace {
		@SerializedName("name") private String m_name;
		@SerializedName("href") private String m_href;
		
		GsJsonNameSpace(String ns, String href) {
			m_name = ns;
			m_href = href;
		}
	}
	
	private static class GsJsonStore {
		@SerializedName("@class") private final String m_class = "dataStore";
		@Skip private final String m_id;
		@SerializedName("name") private final String m_name;
		@SerializedName("href") private final String m_href;
		
		GsJsonStore(String workspace, String storeName, String urlPrefix) {
			m_id = storeName;
			m_name = workspace + ":" + storeName;
			m_href = String.format("%s/workspaces/%s/datastores/%s.json", urlPrefix, workspace, storeName);
		}
	}
	
	private static class GsJsonKeywords {
		@SerializedName("string") private String[] m_keywords;
		
		GsJsonKeywords(String id) {
			m_keywords = new String[] {"features", id };
		}
	}
	
	private static class GsJsonNativeCRS {
		@SerializedName("@class") private String m_class = "projected";
		@SerializedName("$") private String m_crsWkt;
		
		GsJsonNativeCRS(CoordinateReferenceSystem crs) {
			m_crsWkt = crs.toWKT();
		}
	}
	
	private static class GsJsonBoundingBox {
		@SerializedName("minx") private double m_minx;
		@SerializedName("maxx") private double m_maxx;
		@SerializedName("miny") private double m_miny;
		@SerializedName("maxy") private double m_maxy;
		@SerializedName("crs") private String m_crs;
		
		GsJsonBoundingBox(Envelope envl, String crs) {
			m_minx = envl.getMinX();
			m_maxx = envl.getMaxX();
			m_miny = envl.getMinY();
			m_maxy = envl.getMaxY();
			m_crs = crs;
		}
	}
	
	private static class GsJsonAttribute {
		@SerializedName("name") private final String m_name;
		@SerializedName("minOccurs") private final int m_minOccurs;
		@SerializedName("maxOccurs") private final int m_maxOccurs;
		@SerializedName("nillable") private final boolean m_nillable;
		@SerializedName("binding") private final String m_binding;
		
		private GsJsonAttribute(Column col) {
			m_name = col.name();
			m_minOccurs = 0;
			m_maxOccurs = 1;
			m_nillable = true;
			m_binding = col.type().getInstanceClass().getName();
		}
	}
	
	private static class GsJsonAttributes {
		@SerializedName("attribute") private GsJsonAttribute[] m_attr;
		
		private GsJsonAttributes(GsJsonAttribute[] attrs) {
			m_attr = attrs;
		}
		
		private static GsJsonAttributes from(RecordSchema schema) {
			return new GsJsonAttributes(schema.streamColumns()
												.map(GsJsonAttribute::new)
												.toArray(GsJsonAttribute.class));
		}
	}

}
