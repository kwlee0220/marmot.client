package marmot.geoserver.plugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.protobuf.PBUtils;
import utils.fostore.FileObjectHandler;
import utils.fostore.FileObjectStore;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CachingDataSet {
	private static final Logger s_logger = LoggerFactory.getLogger(CachingDataSet.class);

	private final DataSet m_ds;
	private final GeometryColumnInfo m_gcInfo;
	private final FileObjectStore<RecordSet> m_store;
	
	public CachingDataSet(DataSet ds, File cacheDir) {
		Objects.requireNonNull(ds, "DataSet is null");
		Objects.requireNonNull(cacheDir, "cache directory is null");
		
		m_ds = ds;
		m_gcInfo = m_ds.getGeometryColumnInfo();
				
		m_store = new FileObjectStore<>(cacheDir, new ClusterFileHandler(cacheDir), 0);
	}
	
	public MarmotRuntime getMarmotRuntime() {
		return m_ds.getMarmotRuntime();
	}
	
	public String getId() {
		return m_ds.getId();
	}

	public RecordSchema getRecordSchema() {
		return m_ds.getRecordSchema();
	}
	
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_gcInfo;
	}
	
	public Envelope getBounds() {
		return m_ds.getBounds();
	}
	
	public long getRecordCount() {
		return m_ds.getRecordCount();
	}
	
	public RecordSet query(Envelope bounds, Option<String> filter) {
		return m_ds.queryRange(bounds, filter);
	}
	
	public List<SpatialClusterInfo> querySpatialClusterInfo(Envelope range) {
		return m_ds.querySpatialClusterInfo(range);
	}
	
	public boolean isCached(String quadKey) {
		return m_store.getFileObjectIdAll()
						.exists(id -> id.equals(quadKey));
	}
	
	public RecordSet readCluster(String quadKey) {
		if ( !m_store.exists(quadKey) ) {
			s_logger.info("request cluster from the server: quad_key={}", quadKey);
			
			try ( RecordSet rset = m_ds.readSpatialCluster(quadKey, Option.none()) ) {
				s_logger.info("receiving cluster: quad_key={}", quadKey);
				
				m_store.insert(quadKey, rset);
				s_logger.info("stored cluster: quad_key={}", quadKey);
			}
			catch ( Exception e2 ) {
				throw new GSPException("" + e2);
			}
		}
		
		return m_store.get(quadKey);
	}
	
	public SpatialIndexInfo getDefaultSpatialIndexInfoOrNull() {
		return m_ds.getDefaultSpatialIndexInfoOrNull();
	}

	private class ClusterFileHandler implements FileObjectHandler<RecordSet> {
		private final File m_rootDir;
		
		ClusterFileHandler(File rootDir) {
			m_rootDir = rootDir;
		}

		@Override
		public RecordSet readFileObject(File file) throws IOException {
			return PBUtils.readRecordSet(new FileInputStream(file));
		}

		@Override
		public void writeFileObject(RecordSet rset, File file) throws IOException {
			try ( FileOutputStream fos = new FileOutputStream(file) ) {	// just for auto-close
				PBUtils.write(rset, fos);
			}
			finally {
				rset.closeQuietly();
			}
		}

		@Override
		public File toFile(String id) {
			return new File(m_rootDir, id);
		}

		@Override
		public String toFileObjectId(File file) {
			return file.getName();
		}

		@Override
		public boolean isVallidFile(File file) {
			return true;
		}
	}
}