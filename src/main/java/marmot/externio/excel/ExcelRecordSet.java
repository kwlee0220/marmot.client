package marmot.externio.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.monitorjbl.xlsx.StreamingReader;

import marmot.Record;
import marmot.RecordSchema;
import marmot.rset.AbstractRecordSet;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.UnitUtils;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExcelRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(ExcelRecordSet.class);
	private static final int DEF_BUFFER_SIZE = (int)UnitUtils.parseByteSize("16kb");
	
	private final ExcelParameters m_params;
	private final Workbook m_workbook;
	private final RecordSchema m_schema;
	private final Iterator<Row> m_iter;
	private Row m_first = null;
	
	public static ExcelRecordSet from(InputStream is, ExcelParameters params)
		throws EncryptedDocumentException, InvalidFormatException, IOException {
		return new ExcelRecordSet(is, params);
	}
	
	public static ExcelRecordSet from(File file, ExcelParameters params)
		throws EncryptedDocumentException, InvalidFormatException, IOException {
		return new ExcelRecordSet(new FileInputStream(file), params);
	}
	
	private ExcelRecordSet(InputStream is, ExcelParameters params)
		throws IOException, EncryptedDocumentException, InvalidFormatException {
		m_params = params;
		
		m_workbook = StreamingReader.builder()
									.bufferSize(DEF_BUFFER_SIZE)
									.open(is);

		Sheet sheet = m_workbook.getSheetAt(0);
		
		m_iter = sheet.iterator();
		if ( m_params.headerFirst() ) {
			Row header = m_iter.next();
			m_first = null;
			m_schema = readRecordSchema(header);
		}
		else {
			m_first = m_iter.next();
			m_schema = createUnnamedRecordSchema(m_first);
		}
	}
	
	@Override
	protected void closeInGuard() {
		IOUtils.closeQuietly(m_workbook);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		Row row;
		
		if ( m_first != null ) {
			row = m_first;
			m_first = null;
		}
		else {
			if ( !m_iter.hasNext() ) {
				return false;
			}
		
			row = m_iter.next();
		}
		
		readRow(row, output);
		return true;
	}
	
	private RecordSchema readRecordSchema(Row header) {
		RecordSchema.Builder builder = RecordSchema.builder();
		
		for ( int i =0; i < header.getPhysicalNumberOfCells(); ++i ) {
			Cell cell = header.getCell(i);
			String name = (String)readCell(cell, DataType.STRING, i);
			name = adaptColumnName(name);
			
			builder.addColumn(name, DataType.STRING);
		}
		
		return builder.build();
	}
	
	private RecordSchema createUnnamedRecordSchema(Row row) {
		RecordSchema.Builder builder = RecordSchema.builder();
		
		for ( int i =0; i < row.getPhysicalNumberOfCells(); ++i ) {
			String colName = String.format("a%02d", i);
			builder.addColumn(colName, DataType.STRING);
		}
		
		return builder.build();
	}
	
	private void readRow(Row row, Record output) {
		for ( int i =0; i < row.getPhysicalNumberOfCells(); ++i ) {
			Cell cell = row.getCell(i);
	
			if ( cell == null ) {
				output.set(i, null);
			}
			else {
				Object v = readCell(cell, output.getSchema().getColumnAt(i).type(), i);
				output.set(i, v);
			}
		}
	}
	
	private Object readCell(Cell cell, DataType colType, int idx) {
		switch ( cell.getCellTypeEnum() ) {
			case STRING:
				String colStr = cell.getStringCellValue();
				boolean isNull = m_params.nullString()
										.map(str -> str.equals(colStr))
										.getOrElse(false);
				return (isNull) ? null : DataUtils.cast(colStr, colType);
			case NUMERIC:
				return DataUtils.cast(cell.getNumericCellValue(), colType);
			case BOOLEAN:
				return DataUtils.cast(cell.getBooleanCellValue(), colType);
			default:
				return null;
		}
	}
	
	private String adaptColumnName(String colName) {
		// 컬럼 이름을 소문자로 변경시킨다.
		colName = colName.toLowerCase();
		
		// marmot에서는 컬럼이름에 '.'이 들어가는 것을 허용하지 않기 때문에
		// '.' 문자를 '_' 문제로 치환시킨다.
		if ( colName.indexOf(".") >= 0 ) {
			String replaced = colName.replaceAll("\\.", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			
			colName = replaced;
		}
		
		if ( colName.indexOf(" ") >= 0 ) {
			String replaced = colName.replaceAll(" ", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			
			colName = replaced;
		}
		
		return colName;
	}
}
