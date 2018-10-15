package marmot;


import org.junit.Assert;
import org.junit.Test;

import marmot.protobuf.ProtoBufActivator;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchemaTest {
	@Test
	public void test0() throws Exception {
		RecordSchema schema = RecordSchema.EMPTY;
		RecordSchema schema2 = new RecordSchema();
		
		Assert.assertEquals(0, schema.getColumnCount());
		Assert.assertEquals(0, schema2.getColumnCount());
	}
	
	@Test
	public void test1() throws Exception {
		RecordSchema schema = RecordSchema.builder()
											.addColumn("a", DataType.INT)
											.addColumn("b", DataType.STRING)
											.build();
		RecordSchema schema2 = (RecordSchema)ProtoBufActivator.activate(schema.toProto());
		
		Assert.assertEquals(2, schema2.getColumnCount());
		Assert.assertEquals("a", schema2.getColumn("a").name());
		Assert.assertEquals("b", schema2.getColumn("b").name());
	}
	
	@Test
	public void test2() throws Exception {
		RecordSchema schema = RecordSchema.EMPTY;
		RecordSchema schema2 = (RecordSchema)ProtoBufActivator.activate(schema.toProto());

		Assert.assertEquals(0, schema2.getColumnCount());
	}
}
