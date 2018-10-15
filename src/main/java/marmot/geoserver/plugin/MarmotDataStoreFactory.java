package marmot.geoserver.plugin;

import java.awt.RenderingHints.Key;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotDataStoreFactory implements DataStoreFactorySpi {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotDataStoreFactory.class);
	
	private static final Param MARMOT_HOST = new Param("host", String.class,
														"marmot host ip address", true);
	private static final Param MARMOT_PORT = new Param("port", Integer.class,
														"marmot port number", true);
	private static final Param MARMOT_PREFIXES = new Param("prefixes", String.class,
														"target Marmot directory prefixes", false);
	private static final Param MARMOT_SAMPLE_COUNT = new Param("sample_count", Integer.class,
																"sample count", true, -1);
	
	public static final void main(String... args) {
		
	}
	
	public MarmotDataStoreFactory() {
	}

	@Override
	public String getDisplayName() {
		return "marmot";
	}

	@Override
	public String getDescription() {
		return "marmot dataset";
	}

	@Override
	public Param[] getParametersInfo() {
		return new Param[]{
			MarmotDataStoreFactory.MARMOT_HOST,
			MarmotDataStoreFactory.MARMOT_PORT,
			MarmotDataStoreFactory.MARMOT_PREFIXES,
			MarmotDataStoreFactory.MARMOT_SAMPLE_COUNT,
		};
	}

	@Override
	public boolean canProcess(Map<String, Serializable> params) {
		try {
			String host = (String)MarmotDataStoreFactory.MARMOT_HOST.lookUp(params);
			int port = (int)MarmotDataStoreFactory.MARMOT_PORT.lookUp(params);
			
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			marmot.disconnect();
			
			return true;
		}
		catch ( IOException ignored ) { }
		
		return false;
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
		String host = (String)MarmotDataStoreFactory.MARMOT_HOST.lookUp(params);
		int port = (int)MarmotDataStoreFactory.MARMOT_PORT.lookUp(params);

		String[] prefixes = new String[0];
		String prefixesStr = (String)MarmotDataStoreFactory.MARMOT_PREFIXES.lookUp(params);
		if ( prefixesStr != null ) {
			prefixes = CSV.parseAsArray(prefixesStr, ',', '\\');
		}

		long sampleCount = (int)MarmotDataStoreFactory.MARMOT_SAMPLE_COUNT.lookUp(params);
		
		File parentDir = Files.createTempDir().getParentFile();
		File cacheDir = new File(parentDir, "marmot_geoserver_cache");
//		FileUtils.deleteDirectory(cacheDir);
		
		s_logger.info("create MarmotDataStore: cache={}", cacheDir);
		
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		return GSPDataStore.from(marmot, sampleCount, cacheDir, prefixes);
	}

	@Override
	public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
		return null;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return Collections.emptyMap();
	}
}