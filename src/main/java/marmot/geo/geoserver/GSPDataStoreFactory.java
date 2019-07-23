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
	private static final Param DISK_CACHE_DIR = new Param("Disk cache directory", String.class,
														"Disk cache directory", true,
														getDefaultDiskCacheDir().getAbsolutePath());
	private static final Param MARMOT_SAMPLE_COUNT = new Param("Sample count", Integer.class,
																"Sample count", false, 50000);
	private static final Param USE_PREFETCH = new Param("Enable background prefetch", Boolean.class,
														"Enable background prefetch", false, false);
	private static final Param MAX_LOCAL_CACHE_COST = new Param("Max. local cache cost", Integer.class,
														"Max. local cache cost (1~)", false, 20);
	
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
			GSPDataStoreFactory.DISK_CACHE_DIR,
			GSPDataStoreFactory.MARMOT_SAMPLE_COUNT,
			GSPDataStoreFactory.USE_PREFETCH,
			GSPDataStoreFactory.MAX_LOCAL_CACHE_COST,
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
		
		File cacheDir;
		String cacheDirPath = (String)DISK_CACHE_DIR.lookUp(params);
		if ( cacheDirPath == null ) {
			File parentDir = Files.createTempDir().getParentFile();
			cacheDir = new File(parentDir, "marmot_geoserver_cache");
		}
		else {
			cacheDir = new File(cacheDirPath);
		}
		
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		GSPDataStore store = new GSPDataStore(marmot, cacheDir);

		Integer sampleCount = (Integer)MARMOT_SAMPLE_COUNT.lookUp(params);
		if ( sampleCount != null ) {
			store.setSampleCount(sampleCount);
		}

		String[] prefixes = new String[0];
		String prefixesStr = (String)DATASET_PREFIXES.lookUp(params);
		if ( prefixesStr != null ) {
			prefixes = CSV.parseCsvAsArray(prefixesStr);
		}
		store.datasetPrefixes(prefixes);
		
		boolean usePrefetch = (Boolean)USE_PREFETCH.lookUp(params);
		store.usePrefetch(usePrefetch);
		
		Integer maxCost = (Integer)MAX_LOCAL_CACHE_COST.lookUp(params);
		if ( maxCost != null ) {
			store.setMaxLocalCacheCost(maxCost);
		}
		
		s_logger.info("create MarmotDataStore: cache[dir={}], sample_count={}, "
					+ "prefetch={}", cacheDir, sampleCount, usePrefetch);
		
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