package marmot.geo.geoserver;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DataSetPartitionCache;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPDataStore extends ContentDataStore {
	static final Logger s_logger = LoggerFactory.getLogger(GSPDataStore.class);
	private static final String WORKSPACE_URI = "http://marmot.etri.re.kr";
	
	private final PBMarmotClient m_marmot;
	private final LoadingCache<String, GSPDataSetInfo> m_dsCache;
	private final DataSetPartitionCache m_cache;
	private int m_sampleCount = -1;
	private String[] m_prefixes = new String[0];
	
	public GSPDataStore(PBMarmotClient marmot, long cacheSize, int evicMinutes, File cacheDir) {
		Objects.requireNonNull(marmot, "MarmotRuntime");
		Objects.requireNonNull(cacheDir, "Disk cache directory");
		Preconditions.checkArgument(evicMinutes > 0, "Eviction time");
		Preconditions.checkArgument(cacheSize >= 64*1024*1024,
						"Too small memory cache size: " + UnitUtils.toByteSizeString(cacheSize));
		
		m_marmot = marmot;
		m_cache = DataSetPartitionCache.builder()
										.fileStoreDir(cacheDir)
										.maxSize(cacheSize)
										.timeout(evicMinutes, TimeUnit.MINUTES)
										.build(marmot);
		
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(60, TimeUnit.MINUTES)
								.build(new CacheLoader<String,GSPDataSetInfo>() {
									@Override
									public GSPDataSetInfo load(String dsId) throws Exception {
										DataSet ds = m_marmot.getDataSet(dsId);
										s_logger.info("load: dataset={}", dsId);
										return new GSPDataSetInfo(ds);
									}
								});
		
		setNamespaceURI(WORKSPACE_URI);
	}
	
	public MarmotRuntime getMarmotRuntime() {
		return m_marmot;
	}
	
	public long getSampleCount() {
		return m_sampleCount;
	}
	
	public GSPDataStore sampleCount(int count) {
		m_sampleCount = count;
		return this;
	}
	
	public GSPDataStore datasetPrefixes(String... prefixes) {
		m_prefixes = prefixes;
		return this;
	}
	
	@Override
    public SimpleFeatureSource getFeatureSource(Name typeName)  throws IOException {
        return getFeatureSource(typeName.getLocalPart(), Transaction.AUTO_COMMIT);
    }

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry entry)
		throws IOException {
		String dsId = GSPUtils.toDataSetId(entry.getTypeName());
		
		GSPDataSetInfo dsInfo = m_dsCache.getUnchecked(dsId);
		GSPFeatureSource source = new GSPFeatureSource(entry, dsInfo, m_cache);
		if ( m_sampleCount > 0 ) {
			source.setSampleCount(m_sampleCount);
		}
		
		return source;
	}

	@Override
	protected List<Name> createTypeNames() throws IOException {
		return FStream.of(m_marmot.getDataSetAll())
						.filter(DataSet::hasGeometryColumn)
						.map(DataSet::getId)
						.filter(id -> !id.startsWith("/tmp/"))
						.filter(id -> {
							if ( m_prefixes.length > 0 ) {
								for ( String prefix: m_prefixes ) {
									if ( id.startsWith(prefix) ) {
										return true;
									}
								}
								return false;
							}
							else {
								return true;
							}
						})
						.map(GSPUtils::toSimpleFeatureTypeName)
						.map(name -> (Name)new NameImpl(name))
						.toList();
	}
}
