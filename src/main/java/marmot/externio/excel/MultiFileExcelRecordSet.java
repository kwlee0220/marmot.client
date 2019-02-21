package marmot.externio.excel;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.rset.ConcatedRecordSet;
import utils.io.FileUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiFileExcelRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileExcelRecordSet.class);
	
	private final File m_start;
	private final FStream<File> m_files;
	private final ExcelParameters m_params;
	private ExcelRecordSet m_first;
	private final RecordSchema m_schema;
	
	public MultiFileExcelRecordSet(File start, ExcelParameters params) {
		Objects.requireNonNull(params);
		
		m_start = start;
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, "**/*.xlsx").toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no Excel files to read: path=" + start);
			}
			
			getLogger().info("loading ExcelFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.from(files);
			m_params = params;
			
			m_first = loadNext();
			m_schema = m_first.getRecordSchema();
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to parse Excel, cause=" + e);
		}
	}
	
	@Override
	protected void closeInGuard() {
		if ( m_first != null ) {
			m_first.closeQuietly();
			m_first = null;
		}
		m_files.closeQuietly();
		
		super.closeInGuard();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_params);
	}

	@Override
	protected ExcelRecordSet loadNext() {
		if ( m_first != null ) {
			ExcelRecordSet rset = m_first;
			m_first = null;
			
			return rset;
		}
		else {
			return m_files.next()
							.map(this::loadExcelFile)
							.getOrNull();
		}
	}
	
	private ExcelRecordSet loadExcelFile(File file) {
		try {
			ExcelRecordSet rset = ExcelRecordSet.from(file, m_params);
			getLogger().info("loading: Excel[{}], from={}", m_params, file);
			
			return rset;
		}
		catch ( Exception e ) {
			getLogger().warn("fails to load ExcelRecordSet: from=" + file + ", cause=" + e);
			throw new RecordSetException("" + e);
		}
	}
}