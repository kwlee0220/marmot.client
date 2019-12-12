package marmot.geo.geoserver;

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

import marmot.MarmotRuntime;
import marmot.dataset.DataSet;
import marmot.geo.query.GeoDataStore;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GSPDataStore extends ContentDataStore {
	static final Logger s_logger = LoggerFactory.getLogger(GSPDataStore.class);
	private static final String WORKSPACE_URI = "http://marmot.etri.re.kr";
	
	private final GeoDataStore m_store;
	private String[] m_prefixes = new String[0];
	
	public GSPDataStore(MarmotRuntime marmot, File cacheDir) throws IOException {
		Utilities.checkNotNullArgument(marmot, "MarmotRuntime is null");
		Utilities.checkNotNullArgument(cacheDir, "Disk cache directory is null");
		
		m_store = GeoDataStore.from(marmot, cacheDir);
		setNamespaceURI(WORKSPACE_URI);
	}
	
	public GSPDataStore setSampleCount(int count) {
		m_store.setSampleCount(count);
		return this;
	}
	
	public GSPDataStore setMaxLocalCacheCost(int cost) {
		m_store.setMaxLocalCacheCost(cost);
		return this;
	}
	
	public GSPDataStore usePrefetch(boolean flag) {
		m_store.setUsePrefetch(flag);
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
		return new GSPFeatureSource(entry, m_store, dsId);
	}

	@Override
	protected List<Name> createTypeNames() throws IOException {
		return FStream.from(m_store.getGeoDataSetAll())
						.map(DataSet::getId)
						.filter(this::filterDataSetId)
						.map(GSPUtils::toSimpleFeatureTypeName)
						.map(name -> (Name)new NameImpl(name))
						.toList();
	}
	
	private boolean filterDataSetId(String dsId) {
		if ( dsId.startsWith("/tmp/") ) {
			return false;
		}
		if ( m_prefixes.length > 0 ) {
			for ( String prefix: m_prefixes ) {
				if ( dsId.startsWith(prefix) ) {
					return true;
				}
			}

			return false;
		}
		else {
			return true;
		}
	}
}
