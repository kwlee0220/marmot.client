package marmot.support;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GoogleGeoCoder implements GeoCoder {
	private static final String URI = "";
	
	public static final void main(String... args) throws Exception {
		GoogleGeoCoder coder = new GoogleGeoCoder();
		System.out.println(coder.getWgs84Location("경기도 부천시 원미구 상동로 90"));
	}
	
	public List<Coordinate> getWgs84Location(String address) throws Exception {
		URI uri = new URIBuilder()
						.setScheme("http")
						.setHost("maps.googleapis.com")
						.setPath("/maps/api/geocode/json")
						.setParameter("address", address)
						.setParameter("language", "ko")
						.setParameter("sensor", "false")
						.setCharset(StandardCharsets.UTF_8)
						.build();
		HttpGet get = new HttpGet(uri);
		try ( CloseableHttpClient client = HttpClients.createDefault();
				CloseableHttpResponse resp = client.execute(get) ) {
			StatusLine status = resp.getStatusLine();
			if ( status.getStatusCode() == 400 ) {
				return null;
			}
			if ( status.getStatusCode() != 200 ) {
				throw new IOException("Http Status Code: " + status.getStatusCode());
			}
			
			String json = EntityUtils.toString(resp.getEntity());
			Map<String,Object> mapped = JsonParser.parse(json);
			String msg = (String)mapped.get("status");
			if ( !msg.equals("OK") ) {
				return null;
			}
			
			mapped = (Map<String,Object>)((List<Object>)mapped.get("results")).get(0);
			mapped = (Map<String,Object>)mapped.get("geometry");
			mapped = (Map<String,Object>)mapped.get("location");
			double lat = (double)mapped.get("lat");
			double lon = (double)mapped.get("lng");
			
			return Lists.newArrayList(new Coordinate(lat, lon));
		}
	}
}
