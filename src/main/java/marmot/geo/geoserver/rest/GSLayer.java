package marmot.geo.geoserver.rest;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSLayer {
	private String m_name;
	private String m_href;
	
	public GSLayer(String name, String href) {
		this.m_name = name;
		this.m_href = href;
	}
	
	@Override
	public String toString() {
		return m_name + ":" + m_href;
	}
}
