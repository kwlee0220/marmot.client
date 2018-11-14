package marmot.geo.geoserver;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import utils.io.IOUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class GSPFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {
	private final SimpleFeatureType m_sfType;
	private final SimpleFeatureIterator m_sfIter;
	
	GSPFeatureReader(SimpleFeatureType sfType, SimpleFeatureIterator iter) {
		m_sfType = sfType;
		m_sfIter = iter;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(m_sfIter);
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return m_sfType;
	}

	@Override
	public SimpleFeature next() {
		return m_sfIter.next();
	}

	@Override
	public boolean hasNext() throws IOException {
		return m_sfIter.hasNext();
	}
}