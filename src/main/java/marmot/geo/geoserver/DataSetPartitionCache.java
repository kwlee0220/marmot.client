package marmot.geo.geoserver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSet;
import marmot.rset.PBInputStreamRecordSet;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;
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
	
	private final LoadingCache<PartitionKey,Partition> m_cache;
	private final FileObjectStore<PartitionKey,InputStream> m_diskCache;

	DataSetPartitionCache(LoadingCache<PartitionKey,Partition> cache,
						FileObjectStore<PartitionKey,InputStream> diskCache) {
		m_cache = cache;
		m_diskCache = diskCache;
	}
	
	public boolean existsAtDisk(String dsId, String quadKey) {
		return m_diskCache.exists(new PartitionKey(dsId, quadKey));
	}
	
	public boolean existsAtMemory(String dsId, String quadKey) {
		return m_cache.getIfPresent(new PartitionKey(dsId, quadKey)) != null;
	}
	
	public List<Record> get(String dsId, String quadKey) {
		return m_cache.getUnchecked(new PartitionKey(dsId, quadKey)).m_records;
	}
	
	public void put(String dsId, String quadKey, InputStream is) {
		try {
			m_diskCache.insert(new PartitionKey(dsId, quadKey), is, false);
		}
		catch ( Exception ignored ) { }
		finally {
			IOUtils.closeQuietly(is);
		}
	}
	
	public void printDebug() {
		int total = 0;
		for ( Entry<PartitionKey,Partition> ent: m_cache.getAllPresent(m_cache.asMap().keySet()).entrySet() ) {
			s_logger.trace("cached: {}, size={}", ent.getKey(), ent.getValue().m_length);
			total += ent.getValue().m_length;
		}
		s_logger.trace("total memory cache size: {}", UnitUtils.toByteSizeString(total));
	}

	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private File m_storeDir;
		private FOption<Long> m_maxCacheSize = FOption.empty();
		private FOption<Tuple2<Long,TimeUnit>> m_timeout = FOption.empty();
		
		public DataSetPartitionCache build(MarmotRuntime marmot) {
			FileObjectStore<PartitionKey,InputStream> diskCache
					= new FileObjectStore<>(m_storeDir, new ParitionFileHandler(m_storeDir));
			
			CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
																.softValues();
			m_maxCacheSize.ifPresent(nbytes -> {
				builder.maximumWeight(nbytes);
				builder.weigher(new PartitionWeigher());
			});
			m_timeout.ifPresent(t -> builder.expireAfterAccess(t._1, t._2));
			builder.removalListener(noti -> {
				s_logger.debug("partition is evicted: key={}", noti.getKey());
			});
			
			ParitionLoader loader = new ParitionLoader(marmot, diskCache);
			LoadingCache<PartitionKey,Partition> cache = builder.build(loader);
			
			return new DataSetPartitionCache(cache, diskCache);
		}
		
		public Builder fileStoreDir(File rootDir) {
			m_storeDir = rootDir;
			return this;
		}
		
		public Builder maxSize(long nbytes) {
			Preconditions.checkArgument(nbytes > 0, "maximum InMemoryParitionCache size");
			
			m_maxCacheSize = FOption.of(nbytes);
			return this;
		}
		
		public Builder timeout(long timeout, TimeUnit unit) {
			m_timeout = FOption.of(Tuple.of(timeout, unit));
			return this;
		}
	}
	
	private static class Partition {
		private final List<Record> m_records;
		private final int m_length;
		
		Partition(List<Record> records, int length) {
			m_records = records;
			m_length = length;
		}
	}
	
	private static class PartitionKey {
		private final String m_dsId;
		private final String m_quadKey;
		
		PartitionKey(String dsId, String quadKey) {
			Objects.requireNonNull(dsId, "DataSet id");
			Objects.requireNonNull(quadKey, "quad-key");
			
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
	
	private static class PartitionWeigher implements Weigher<PartitionKey,Partition> {
		@Override
		public int weigh(PartitionKey key, Partition parition) {
			return (int)parition.m_length;
		}
	}
	
	private static class ParitionLoader extends CacheLoader<PartitionKey,Partition> {
		private final MarmotRuntime m_marmot;
		private final LoadingCache<String,DataSet> m_dsCache;
		private final FileObjectStore<PartitionKey,InputStream> m_fileStore;
		
		private ParitionLoader(MarmotRuntime marmot,
								FileObjectStore<PartitionKey,InputStream> fileStore) {
			m_marmot = marmot;
			m_fileStore = fileStore;
			
			m_dsCache = CacheBuilder.newBuilder()
									.expireAfterAccess(30, TimeUnit.MINUTES)
									.removalListener(this::onDataSetRemoved)
									.build(new CacheLoader<String,DataSet>() {
										@Override
										public DataSet load(String key) throws Exception {
											return m_marmot.getDataSet(key);
										}
									});
		}
		
		@Override
		public Partition load(PartitionKey key) throws Exception {
			try {
				return m_fileStore.getFile(key)
									.mapTE(this::loadFromFileCache)
									.getOrElseTE(() -> loadFromServer(key));
			}
			catch ( Throwable e ) {
				throw Throwables.asException(e);
			}
		}
		
		private Partition loadFromFileCache(File file) throws IOException {
			StopWatch watch = StopWatch.start();
			Partition part = fromFile(file);
			watch.stop();
			
			if ( s_logger.isDebugEnabled() ) {
				String elapsedStr = watch.getElapsedSecondString();
				double elapsed = watch.getElapsedInFloatingSeconds();
				String veloStr = String.format("%.0f", part.m_records.size() / elapsed);
				
				s_logger.debug("load from fileCache: path={}, size={}, elapsed={}, velo={}/s",
								file.getAbsolutePath(),
								UnitUtils.toByteSizeString(part.m_length),
								elapsedStr, veloStr);
			}
			
			return part;
		}
		
		private Partition loadFromServer(PartitionKey key)
			throws FileObjectExistsException, IOException {
			StopWatch watch = StopWatch.start();
			
			DataSet ds = m_dsCache.getUnchecked(key.m_dsId);
			InputStream cluster = ds.readRawSpatialCluster(key.m_quadKey);
			File file = m_fileStore.insert(key, cluster);
			Partition part = fromFile(file);
			
			watch.stop();
			
			if ( s_logger.isDebugEnabled() ) {	
				String elapsedStr = watch.getElapsedSecondString();
				double elapsed = watch.getElapsedInFloatingSeconds();
				String veloStr = String.format("%.0f", part.m_records.size() / elapsed);
				
				s_logger.debug("load from MarmotServer: ds={}, quad_key={}, size={}, elapsed={}, velo={}/s",
								key.m_dsId, key.m_quadKey,
								UnitUtils.toByteSizeString(part.m_length),
								elapsedStr, veloStr);
			}
			
			return part;
		}
		
		private Partition fromFile(File file) throws IOException {
			try ( InputStream is = new BufferedInputStream(new FileInputStream(file));
				RecordSet rset = PBInputStreamRecordSet.from(is) ) {
				return new Partition(rset.toList(), (int)file.length());
			}
		}
		
		private void onDataSetRemoved(RemovalNotification<String,DataSet> noti) {
			String dsId = noti.getKey();
			
			s_logger.info("victim selected: dataset={}", dsId);
			
			try {
				List<PartitionKey> keys = m_fileStore.traverse()
													.filter(k -> k.m_dsId.equals(dsId))
													.collect(Collectors.toList());
				
				for ( PartitionKey key: keys ) {
					m_fileStore.remove(key);
				}
				
				m_dsCache.invalidateAll(keys);
			}
			catch ( IOException ignored ) { }
		}
	}
	
	private static class ParitionFileHandler
							implements FileObjectHandler<PartitionKey, InputStream> {
		private final File m_rootDir;
		
		ParitionFileHandler(File rootDir) {
			m_rootDir = rootDir;
		}

		@Override
		public InputStream readFileObject(File file) throws IOException {
			Objects.requireNonNull(file, "FileStore file");
			
			return new FileInputStream(file);
		}

		@Override
		public void writeFileObject(InputStream block, File file) throws IOException {
			Objects.requireNonNull(block, "InputStream");
			Objects.requireNonNull(file, "FileStore file");
			
			IOUtils.toFile(block, file);
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
