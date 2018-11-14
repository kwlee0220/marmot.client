package marmot.geo.geoserver;

import java.awt.RenderingHints.Key;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPDataStoreFactory implements DataStoreFactorySpi {
	private static final Logger s_logger = LoggerFactory.getLogger(GSPDataStoreFactory.class);
	
	private static final Param MARMOT_HOST = new Param("Marmot server host", String.class,
														"Marmot server host", true, "localhost");
	private static final Param MARMOT_PORT = new Param("Marmot server port", Integer.class,
														"Marmot server port", true, 12985);
	private static final Param DATASET_PREFIXES = new Param("Marmot dataset prefixes", String.class,
														"Marmot dataset prefixes", false);
	private static final Param MEMORY_CACHE_SIZE = new Param("Memory cache size", String.class,
														"Memory cache size", true, "512mb");
	private static final Param EVICTION_TIMEOUT = new Param("Memory cache eviction time (in minute)",
														Integer.class, "minutes", true, 10);
	private static final Param DISK_CACHE_DIR = new Param("Disk cache directory", String.class,
														"Disk cache directory", true,
														getDefaultDiskCacheDir().getAbsolutePath());
	private static final Param MARMOT_SAMPLE_COUNT = new Param("Sample count", Integer.class,
																"Sample count", false, 100000);
	
	public GSPDataStoreFactory() {
	}

	@Override
	public String getDisplayName() {
		return "Marmot";
	}

	@Override
	public String getDescription() {
		return "Marmot (Spatial Big-Data Platform)";
	}

	@Override
	public Param[] getParametersInfo() {
		return new Param[]{
			GSPDataStoreFactory.MARMOT_HOST,
			GSPDataStoreFactory.MARMOT_PORT,
			GSPDataStoreFactory.DATASET_PREFIXES,
			GSPDataStoreFactory.MEMORY_CACHE_SIZE,
			GSPDataStoreFactory.EVICTION_TIMEOUT,
			GSPDataStoreFactory.DISK_CACHE_DIR,
			GSPDataStoreFactory.MARMOT_SAMPLE_COUNT,
		};
	}

	@Override
	public boolean canProcess(Map<String, Serializable> params) {
		try {
			String host = (String)GSPDataStoreFactory.MARMOT_HOST.lookUp(params);
			int port = (int)GSPDataStoreFactory.MARMOT_PORT.lookUp(params);
			
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
		String host = (String)MARMOT_HOST.lookUp(params);
		int port = (int)MARMOT_PORT.lookUp(params);
		
		String cacheSizeStr = (String)MEMORY_CACHE_SIZE.lookUp(params);
		int cacheSize = (int)UnitUtils.parseByteSize(cacheSizeStr);
		
		int timeout = (int)EVICTION_TIMEOUT.lookUp(params);
		
		File parentDir = Files.createTempDir().getParentFile();
		File cacheDir = new File(parentDir, "marmot_geoserver_cache");
		
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		GSPDataStore store = new GSPDataStore(marmot, cacheSize, timeout, cacheDir);

		Integer sampleCount = (Integer)MARMOT_SAMPLE_COUNT.lookUp(params);
		if ( sampleCount != null ) {
			store.sampleCount(sampleCount);
		}

		String[] prefixes = new String[0];
		String prefixesStr = (String)DATASET_PREFIXES.lookUp(params);
		if ( prefixesStr != null ) {
			prefixes = CSV.parseAsArray(prefixesStr, ',', '\\');
		}
		store.datasetPrefixes(prefixes);
		s_logger.info("create MarmotDataStore: cache[size={}, dir={}], sample_count={}",
						cacheSizeStr, cacheDir, sampleCount);
		
		return store;
	}

	@Override
	public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return Collections.emptyMap();
	}
	
	private static File getDefaultDiskCacheDir() {
		File parentDir = Files.createTempDir().getParentFile();
		return new File(parentDir, "marmot_geoserver_cache");
	}
}