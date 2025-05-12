package marmot.geo.geoserver.rest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import utils.CSV;
import utils.Tuple;
import utils.Tuple3;
import utils.stream.FStream;
import utils.stream.KeyValueFStream;

import marmot.dataset.DataSet;
import marmot.geo.geoserver.GSPUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoServer {
	private final String m_urlPrefix;
	private final String m_userId;
	private final String m_passwd;
	private String m_workspace = "marmot";
	private String m_storeName = "marmot";
	
	public static GeoServer create(String host, int port, String userName, String passwd) {
		String url = String.format("http://%s:%d/geoserver/rest", host, port);
		
		return new GeoServer(url, userName, passwd);
	}
	
	public static GeoServer create(String urlPrefix, String userName, String passwd) {
		return new GeoServer(urlPrefix, userName, passwd);
	}
	
	private GeoServer(String urlPrefix, String userId, String passwd) {
		m_urlPrefix = urlPrefix;
		m_userId = userId;
		m_passwd = passwd;
	}
	
	public List<String> listLayers() {
		try {
			Tuple3<Integer, String, String> ret = get(getLayersUrl());
			if ( ret._1 >= 200 && ret._1 < 300 ) {
				JsonParser gson = new JsonParser();
				
				JsonElement layersElm = gson.parse(ret._3).getAsJsonObject()
											.get("layers");
				if ( layersElm.isJsonPrimitive() ) {	// 등록된 layer가 없는 경우
					return Collections.emptyList();
				}
				
				JsonArray layers = layersElm.getAsJsonObject()
											.getAsJsonArray("layer");
				return FStream.from(layers)
								.cast(JsonObject.class)
								.map(o -> o.get("name").getAsString())
								.filter(fn -> fn.startsWith(m_storeName + ":"))
								.map(this::parseLayerName)
								.toList();
			}
			else {
				throw new GeoServerException(ret._2);
			} 
		}
		catch ( IOException | AuthenticationException e ) {
			throw new GeoServerException(e);
		}
	}
	
	public void addLayer(DataSet ds) {
		GsJsonFeatureType type = new GsJsonFeatureType(m_urlPrefix, m_workspace, m_storeName, ds);
		GsJsonFeatureTypeInfo ftInfo = new GsJsonFeatureTypeInfo(type);
		Gson gson =  new GsonBuilder()
						.setExclusionStrategies(new GsJsonExclusionStrategy())
						.create();
		String ftJson = gson.toJson(ftInfo).toString();

		try {
			Tuple3<Integer, String, String> ret = post(getFeatureTypesUrl(), ftJson);
			if ( ret._1 >= 200 && ret._1 < 300 ) {
				return;
			}
			else {
				throw new GeoServerException(ret._2);
			}
		}
		catch ( AuthenticationException | IOException e ) {
			throw new GeoServerException(e);
		}
	}
	
	public boolean removeLayer(String dsId) {
		String ftName = toFeatureTypeName(dsId);
		try {
			Map<String,String> params = Maps.newHashMap();
			params.put("recurse", "true");
			
			Tuple<Integer, String> ret = delete(getFeatureTypeUrl(ftName), params);
			if ( ret._1 >= 200 && ret._1 < 300 ) {
				return true;
			}
			else if ( ret._1 == 404 ) {
				return false;
			}
			else {
				throw new GeoServerException(ret._2);
			}
		}
		catch ( AuthenticationException | IOException | URISyntaxException e ) {
			throw new GeoServerException(e);
		}
	}
	
	private String getLayersUrl() {
		return String.format("%s/layers", m_urlPrefix);
	}
	
	private String getFeatureTypesUrl() {
		return String.format("%s/workspaces/%s/datastores/%s/featuretypes",
							m_urlPrefix, m_workspace, m_storeName);
	}
	
	private String getFeatureTypeUrl(String ftName) {
		return String.format("%s/workspaces/%s/datastores/%s/featuretypes/%s.json",
								m_urlPrefix, m_workspace, m_storeName, ftName);
	}
	
	public String toFeatureTypeName(String dsId) {
		return GSPUtils.toSimpleFeatureTypeName(dsId);
	}
	
	private String toDataSetId(String ftName) {
		return GSPUtils.toDataSetId(ftName);
	}
	
	private Tuple3<Integer,String,String> get(String url)
		throws ClientProtocolException, IOException, AuthenticationException {
		try ( CloseableHttpClient client = HttpClients.createDefault(); ) {
			HttpGet httpGet = new HttpGet(url);
			
		    httpGet.setHeader("Content-type", "application/json");
		    httpGet.setHeader("Accept", "application/json");
		    UsernamePasswordCredentials creds  = new UsernamePasswordCredentials(m_userId, m_passwd);
		    httpGet.addHeader(new BasicScheme().authenticate(creds, httpGet, null));

		    CloseableHttpResponse resp = client.execute(httpGet);
	    	String details = String.format("%s(%d)", resp.getStatusLine().getReasonPhrase(),
														resp.getStatusLine().getStatusCode());
		    int code = resp.getStatusLine().getStatusCode();
		    if ( code >= 200 && code < 300 ) {
			    ResponseHandler<String> handler = new BasicResponseHandler();
			    return Tuple.of(code, details, handler.handleResponse(resp));
		    }
		    else {
		    	return Tuple.of(code, details, (String)null);
		    }
		}
	}
	
	private Tuple3<Integer,String,String> post(String url, String postData)
		throws AuthenticationException, ClientProtocolException, IOException {
		try ( CloseableHttpClient client = HttpClients.createDefault(); ) {
			HttpPost httpPost = new HttpPost(url);
			
			httpPost.setEntity(new StringEntity(postData, ContentType.APPLICATION_JSON));
			httpPost.setHeader("Content-type", "application/json");
			httpPost.setHeader("Accept", "application/json");
		    UsernamePasswordCredentials creds  = new UsernamePasswordCredentials(m_userId, m_passwd);
		    httpPost.addHeader(new BasicScheme().authenticate(creds, httpPost, null));
		    
		    CloseableHttpResponse resp = client.execute(httpPost);
	    	String details = String.format("%s(%d)", resp.getStatusLine().getReasonPhrase(),
											resp.getStatusLine().getStatusCode());
		    int code = resp.getStatusLine().getStatusCode();
		    if ( code >= 200 && code < 300 ) {
			    ResponseHandler<String> handler = new BasicResponseHandler();
			    return Tuple.of(code, details, handler.handleResponse(resp));
		    }
		    else {
		    	return Tuple.of(code, details, null);
		    }
		}
	}
	
	private Tuple<Integer,String> delete(String url, Map<String,String> params)
		throws ClientProtocolException, IOException, AuthenticationException, URISyntaxException {
		URI uri = KeyValueFStream.from(params)
								.fold(new URIBuilder(url), (b,kv) -> b.addParameter(kv.key(), kv.value()))
								.build();

		try ( CloseableHttpClient client = HttpClients.createDefault(); ) {
			HttpDelete httpDelete = new HttpDelete(uri);
			httpDelete.setHeader("Content-type", "application/json");
			httpDelete.setHeader("Accept", "application/json");
		    UsernamePasswordCredentials creds  = new UsernamePasswordCredentials(m_userId, m_passwd);
		    httpDelete.addHeader(new BasicScheme().authenticate(creds, httpDelete, null));
		    
		    CloseableHttpResponse resp = client.execute(httpDelete);
	    	String details = String.format("%s(%d)", resp.getStatusLine().getReasonPhrase(),
													resp.getStatusLine().getStatusCode());
	    	return Tuple.of(resp.getStatusLine().getStatusCode(), details);
		}
	}
	
	private String parseLayerName(String fullName) {
		String ftName = CSV.parseCsv(fullName, ':').toList().get(1);
		return toDataSetId(ftName);
	}
}
