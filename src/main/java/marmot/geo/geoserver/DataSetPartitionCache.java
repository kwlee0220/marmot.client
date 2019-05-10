package marmot.geo.geoserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.RecordSet;
import marmot.rset.PBInputStreamRecordSet;
import marmot.rset.PBRecordSetInputStream;
import utils.Utilities;
import utils.fostore.FileObjectExistsException;
import utils.fostore.FileObjectHandler;
import utils.fostore.FileObjectStore;
import utils.func.FOption;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetPartitionCache {
	private static final Logger s_logger = LoggerFactory.getLogger(DataSetPartitionCache.class);
	private static final int DS_CACHE_EXPIRE_MINUTES = 30;

	private final LoadingCache<String,DataSet> m_dsCache;
	private final FileObjectStore<PartitionKey,InputStream> m_cache;

	public DataSetPartitionCache(MarmotRuntime marmot, File storeRoot) {
		m_cache = new FileObjectStore<>(storeRoot, new ParitionFileHandler(storeRoot));
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(DS_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
								.removalListener(this::onDataSetRemoved)
								.build(new CacheLoader<String,DataSet>() {
									@Override
									public DataSet load(String key) throws Exception {
										return marmot.getDataSet(key);
									}
								});
	}
	
	public boolean exists(String dsId, String quadKey) {
		return m_cache.exists(new PartitionKey(dsId, quadKey));
	}
	
	public RecordSet get(String dsId, String quadKey) throws IOException {
		PartitionKey key = new PartitionKey(dsId, quadKey);
		FOption<InputStream> ois = m_cache.get(key);
		if ( ois.isPresent() ) {
			return PBInputStreamRecordSet.from(ois.getUnchecked());
		}
		else {
			DataSet ds = m_dsCache.getUnchecked(key.m_dsId);
			RecordSet cluster = ds.readSpatialCluster(key.m_quadKey);
			
			File file = writeIntoCache(key, cluster);
			return PBInputStreamRecordSet.from(new FileInputStream(file));
		}
	}
	
	public void put(String dsId, String quadKey, RecordSet rset)
		throws FileObjectExistsException, IOException {
		writeIntoCache(new PartitionKey(dsId, quadKey), rset);
	}
	
	public void remove(String dsId, String quadKey) {
		m_cache.remove(new PartitionKey(dsId, quadKey));
	}
	
	private File writeIntoCache(PartitionKey key, RecordSet rset)
		throws FileObjectExistsException, IOException {
		rset = RecordSet.from(rset.getRecordSchema(), rset.stream().shuffle());
		return m_cache.insert(key, PBRecordSetInputStream.from(rset));
	}
	
	private void onDataSetRemoved(RemovalNotification<String,DataSet> noti) {
		String dsId = noti.getKey();
		
		s_logger.info("victim selected: dataset={}", dsId);
		
		try {
			List<PartitionKey> keys = m_cache.traverse()
												.filter(k -> k.m_dsId.equals(dsId))
												.collect(Collectors.toList());
			
			for ( PartitionKey key: keys ) {
				m_cache.remove(key);
			}
			
			m_dsCache.invalidateAll(keys);
		}
		catch ( IOException ignored ) { }
	}
	
	private static final class PartitionKey {
		private final String m_dsId;
		private final String m_quadKey;
		
		PartitionKey(String dsId, String quadKey) {
			Utilities.checkNotNullArgument(dsId, "DataSet id");
			Utilities.checkNotNullArgument(quadKey, "quad-key");
			
			m_dsId = dsId;
			m_quadKey = quadKey;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%s", m_dsId, m_quadKey);
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			else if ( obj == null && !(obj instanceof PartitionKey) ) {
				return false;
			}
			
			PartitionKey other = (PartitionKey)obj;
			return m_dsId.equals(other.m_dsId)
				&& m_quadKey.equals(other.m_quadKey);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(m_dsId, m_quadKey);
		}
	}
	
	private static final class ParitionFileHandler
								implements FileObjectHandler<PartitionKey, InputStream> {
		private final File m_rootDir;
		
		ParitionFileHandler(File rootDir) {
			m_rootDir = rootDir;
		}

		@Override
		public InputStream readFileObject(File file) throws IOException {
			Utilities.checkNotNullArgument(file, "FileStore file");
			
			return new FileInputStream(file);
		}

		@Override
		public void writeFileObject(InputStream is, File file) throws IOException {
			Utilities.checkNotNullArgument(is, "InputStream");
			Utilities.checkNotNullArgument(file, "FileStore file");
			
			IOUtils.toFile(is, file);
		}

		@Override
		public File toFile(PartitionKey key) {
			return new File(new File(m_rootDir, key.m_dsId), key.m_quadKey);
		}

		@Override
		public PartitionKey toFileObjectKey(File file) {
			return new PartitionKey(file.getParentFile().getName(), file.getName());
		}

		@Override
		public boolean isVallidFile(File file) {
			return true;
		}
	}
}
