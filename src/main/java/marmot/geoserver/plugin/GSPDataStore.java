package marmot.geoserver.plugin;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import marmot.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPDataStore extends ContentDataStore {
	static final Logger s_logger = LoggerFactory.getLogger(GSPDataStore.class);
	private static final String WORKSPACE_URI = "http://marmot.etri.re.kr";
	
	private final PBMarmotClient m_marmot;
	private final String[] m_prefixes;
	private final File m_rootDir;
	private final FeatureSourceCache m_cache;
	private final long m_sampleCount;
	
	public static GSPDataStore from(PBMarmotClient marmot, long sampleCount, File cacheDir,
										String... prefixes) {
		return new GSPDataStore(marmot, sampleCount, cacheDir, prefixes);
	}
	
	public static GSPDataStore from(PBMarmotClient marmot, long sampleCount, String... prefixes) {
		File parentDir = Files.createTempDir().getParentFile();
		File cacheDir = new File(parentDir, "marmot_geoserver_cache");
		
		return new GSPDataStore(marmot, sampleCount, cacheDir, prefixes);
	}
	
	private GSPDataStore(PBMarmotClient marmot, long sampleCount, File cacheDir, String... prefixes) {
		m_marmot = marmot;
		m_sampleCount = sampleCount;
		m_prefixes = prefixes;
		m_cache = new FeatureSourceCache();
		m_rootDir = cacheDir;
		
		setNamespaceURI(WORKSPACE_URI);
	}
	
	public PBMarmotClient getMarmotRuntime() {
		return m_marmot;
	}
	
	public long getSampleCount() {
		return m_sampleCount;
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
	
	@Override
    public SimpleFeatureSource getFeatureSource(Name typeName)  throws IOException {
        return getFeatureSource(typeName.getLocalPart(), Transaction.AUTO_COMMIT);
    }

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry entry)
		throws IOException {
		String dsId = GSPUtils.toDataSetId(entry.getTypeName());
		return m_cache.computeIfAbsent(dsId, id->createGSPFeatureSource(entry));
	}
	
	private GSPFeatureSource createGSPFeatureSource(ContentEntry entry) {
		String sfTypeName = entry.getTypeName();
		
		String dsId = GSPUtils.toDataSetId(sfTypeName);
		DataSet ds = m_marmot.getDataSet(dsId);
		
		File dsRootDir = new File(m_rootDir, sfTypeName);
		CachingDataSet cachingDs = new CachingDataSet(ds, dsRootDir);
		
		s_logger.info("load: dataset={}", dsId);
		
		GSPFeatureSource source = new GSPFeatureSource(entry, cachingDs);
		if ( m_sampleCount > 0 ) {
			source.setSampleCount(m_sampleCount);
		}
		
		return source;
	}
}
