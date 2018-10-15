package marmot.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import marmot.type.DataType;
import marmot.type.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotIOUtils {
	public static RecordSchema readRecordSchema(DataInput input) throws IOException {
		RecordSchema.Builder builder = RecordSchema.builder();
		
		int ncols = input.readShort();
		for ( int i =0; i < ncols; ++i ) {
			String name = input.readUTF();
			DataType type = DataTypes.fromTypeCode(input.readByte());
			
			builder.addColumn(name, type);
		}
		return builder.build();
	}
	
	public static void writeRecordSchema(RecordSchema schema, DataOutput output) throws IOException {
		output.writeShort(schema.getColumnCount());
		for ( Column col: schema.getColumnAll() ) {
			output.writeUTF(col.name());
			output.writeByte(col.type().getTypeCode().ordinal());
		}
	}
	
	public static void readRecord(DataInput input, Record record) throws IOException {
		RecordSchema schema = record.getSchema();
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			DataType type = schema.getColumnAt(i).type();
			Object value = type.readObject(input);
			record.set(i, value);
		}
	}
	
	public static Record readRecord(DataInput input, RecordSchema schema) throws IOException {
		Record record = DefaultRecord.of(schema);
		readRecord(input, record);
		return record;
	}
	
	public static void writeRecord(Record record, DataOutput output) throws IOException {
		RecordSchema schema = record.getSchema();
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			DataType type = schema.getColumnAt(i).type();
			
			type.writeObject(record.get(i), output);
		}
	}
	
	public static RecordSet readRecordSet(DataInput input, RecordSchema schema) throws IOException {
		return new Loaded(input, schema);
	}
	
	public static void writeRecordSet(RecordSet rset, DataOutput output) throws IOException {
		Record record = DefaultRecord.of(rset.getRecordSchema());
		
		boolean hasMore = rset.next(record);
		while ( hasMore ) {
			output.writeByte(1);
			writeRecord(record, output);
			
			hasMore = rset.next(record);
		}
		output.writeByte(0);
	}

	private static class Loaded extends AbstractRecordSet {
		private final DataInput m_input;
		private final RecordSchema m_schema;
		private boolean m_eor = false;
		
		Loaded(DataInput input, RecordSchema schema) {
			m_input = input;
			m_schema = schema;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			if ( m_eor ) {
				return false;
			}
			
			try {
				boolean hasMore = (m_input.readByte() != 0);
				if ( !hasMore ) {
					m_eor = true;
					return false;
				}
				
				readRecord(m_input, record);
				return true;
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to read record", e);
			}
		}

		@Override
		protected void closeInGuard() { }
	}
}
