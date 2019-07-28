package marmot.geo.geoserver.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import marmot.DataSet;
import marmot.geo.geoserver.GSPUtils;
import utils.func.FOption;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoServer {
	private static final String CONTENT_TYPE = "application/json";
	private static final String ACCEPT = "application/json";
	
	private final String m_urlPrefix;
	private final String m_userId;
	private final String m_passwd;
	private String m_workspace = "marmot";
	private String m_storeName = "marmot";
	
	public static final void main(String... args) throws Exception {
		GeoServer server = new GeoServer("http://220.74.32.5:9987/geoserver/rest", "admin", "geoserver");
//		System.out.println(server.listLayers());
		server.listFeatureTypes();
	}
	
	public static GeoServer create(String host, int port, String userId, String passwd) {
		String url = String.format("http://%s:%d/geoserver/rest", host, port);
		
		return new GeoServer(url, "admin", "geoserver");
	}
	
	public GeoServer(String urlPrefix, String userId, String passwd) {
		m_urlPrefix = urlPrefix;
		m_userId = userId;
		m_passwd = passwd;
	}
	
	public List<GSLayer> listLayers() throws IOException {
		String json = callToString("GET", "/layers", FOption.empty());
		
		JsonParser gson = new JsonParser();
		return FStream.from(gson.parse(json).getAsJsonObject().entrySet())
					.flatMapIterable(v -> ((JsonObject)v.getValue()).entrySet())
					.flatMapIterable(v -> (JsonArray)v.getValue())
					.cast(JsonObject.class)
					.map(o -> new GSLayer(o.get("name").getAsString(), o.get("href").getAsString()))
					.toList();
	}
	
	public void listFeatureTypes() throws IOException {
//		String suffix = String.format("/workspaces/marmot/datastores/marmot/featuretypes/POI.주유소_가격.json");
		String suffix = String.format("/workspaces/marmot/datastores/marmot/featuretypes");
		String str = callToString("GET", suffix, FOption.empty());
		
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(str).getAsJsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		System.out.println(gson.toJson(json));
	}
	
	public void addLayer(DataSet ds) throws IOException, AuthenticationException {
		GsJsonFeatureType type = new GsJsonFeatureType(m_urlPrefix, m_workspace, m_storeName, ds);
		GsJsonFeatureTypeInfo ftInfo = new GsJsonFeatureTypeInfo(type);
		Gson gson =  new GsonBuilder()
						.setPrettyPrinting()
						.setExclusionStrategies(new GsJsonExclusionStrategy())
						.create();
		String ftJson = gson.toJson(ftInfo).toString();
		System.out.println(ftJson);

		int retCode = post(getFeatureTypesUrl(), ftJson);
		System.out.println(retCode);
	}
	
	private String getFeatureTypesUrl() {
		return String.format("%s/workspaces/%s/datastores/%s/featuretypes",
							m_urlPrefix, m_workspace, m_storeName);
	}
	
	private String getFeatureTypeUrl(String ftName) {
		return String.format("%s/workspaces/%s/datastores/%s/featuretypes/%s",
								m_urlPrefix, m_workspace, m_storeName, ftName);
	}
	
	public void removeLayer(String dsId) throws ClientProtocolException, IOException {
		String ftName = toFeatureTypeName(dsId);
		delete(getFeatureTypeUrl(ftName));
	}
	
	public String toFeatureTypeName(String dsId) {
		return GSPUtils.toSimpleFeatureTypeName(dsId);
	}
	
//	public void publishLayer(String layerId);
//	public void unpublishLayer(String layerId);
//	
//	public void quit();

	private String callToString(String method, String urlEncoded, FOption<String> postData)
								throws IOException {
		HttpURLConnection conn = sendREST(method, urlEncoded, postData.map(StringReader::new));
		try ( InputStream in = conn.getInputStream() ) {
			return IOUtils.toString(in, StandardCharsets.UTF_8);
		}
	}
	
	private int delete(String url) throws ClientProtocolException, IOException {
		try ( CloseableHttpClient client = HttpClients.createDefault(); ) {
			HttpDelete httpDelete = new HttpDelete(url);
		    CloseableHttpResponse resp = client.execute(httpDelete);
		    
		    return resp.getStatusLine().getStatusCode();
		}
	}
	
	private int post(String url, String postData)
		throws AuthenticationException, ClientProtocolException, IOException {
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(url);
		
		httpPost.setEntity(new StringEntity(postData, ContentType.APPLICATION_JSON));
		httpPost.setHeader("Content-type", "application/json");
		httpPost.setHeader("Accept", "application/json");
	    UsernamePasswordCredentials creds  = new UsernamePasswordCredentials(m_userId, m_passwd);
	    httpPost.addHeader(new BasicScheme().authenticate(creds, httpPost, null));
	    
	    CloseableHttpResponse resp = client.execute(httpPost);
	    int ret = resp.getStatusLine().getStatusCode();
	    
	    client.close();
	    
	    return ret;
	}
	
	private HttpURLConnection sendREST(String method, String urlAppend,
										FOption<Reader> postDataReader)
		throws MalformedURLException, IOException {
		boolean doOut = !"DELETE".equals(method) && postDataReader.isPresent();
		// boolean doIn = true; // !doOut
		URL url = new URL(m_urlPrefix + urlAppend);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setDoOutput(doOut);
		// uc.setDoInput(false);
		connection.setRequestProperty("Content-type", CONTENT_TYPE);
		connection.setRequestProperty("Accept", ACCEPT);
		connection.setRequestMethod(method);

		if ( m_userId != null && m_passwd != null ) {
			String encoded = Base64.getEncoder().encodeToString((m_userId + ":" + m_passwd).getBytes());
			connection.setRequestProperty("Authorization", "Basic " + encoded);
		}

		connection.connect();
		if ( connection.getDoOutput() ) {
			Reader reader = new BufferedReader(postDataReader.get());
			try ( Writer writer = new OutputStreamWriter(connection.getOutputStream()) ) {
				IOUtils.transfer(reader, writer, 4096);
			}
		}
		
		return connection;
	}
}
