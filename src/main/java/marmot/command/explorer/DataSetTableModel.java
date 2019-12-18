package marmot.command.explorer;

import java.util.ArrayList;
import java.util.List;

import javax.swing.SwingWorker;
import javax.swing.table.AbstractTableModel;

import marmot.Column;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class DataSetTableModel extends AbstractTableModel {
	private static final long serialVersionUID = 1L;

	private final MarmotRuntime m_marmot;
	private List<Column> m_columns = new ArrayList<>();
	private List<Record> m_records = new ArrayList<>();
	
	DataSetTableModel(MarmotRuntime marmot) {
		m_marmot = marmot;
	}

	@Override
	public int getColumnCount() {
		return m_columns.size();
	}

	@Override
	public String getColumnName(int i) {
		return m_columns.get(i).name();
	}

	@Override
	public int getRowCount() {
		return m_records.size();
	}

	@Override
	public Object getValueAt(int rowIndex, int columnIndex) {
		return m_records.get(rowIndex).get(columnIndex);
	}
	
	void update(DataSet ds) {
		new SwingWorker<Void, Object>() {
			@Override
			protected Void doInBackground() throws Exception {
				publish(loadSchema(ds));
				load(ds);
				
				return null;
			}

			@SuppressWarnings("unchecked")
			@Override
			protected void process(List<Object> workList) {
				for ( Object work: workList ) {
					if ( work instanceof Record ) {
						m_records.add((Record)work);
					}
					else if ( work instanceof List ) {
						m_columns = (List<Column>)work;
						m_records.clear();
					}
				}
				fireTableStructureChanged();
			}

			@Override
			protected void done() { }
			
			private void load(DataSet ds2) {
				PlanBuilder builder = Plan.builder("list a dataset")
											.load(ds.getId());
				for ( Column col: ds2.getRecordSchema().getColumns() ) {
					if ( col.type().isGeometryType()
						|| col.type() == DataType.ENVELOPE) {
						builder.defineColumn(col.name() + ":string",
											String.format("'%s'", col.type().toString()));
					}
				}
				
				Plan plan = builder.take(30).build();
				try ( RecordSet rset = m_marmot.executeLocally(plan) ) {
					for ( int i =1; i <= 30; ++i ) {
						Record rec = rset.nextCopy();
						if ( rec == null ) {
							break;
						}
						
						publish(rec);
						setProgress((int)Math.round((i/30d) * 100));
					}
					setProgress(100);
				}
			}
		}.execute();
	}
	
	private List<Column> loadSchema(DataSet ds) {
		return ds.getRecordSchema()
				.streamColumns()
				.toList();
	}

	void clear() {
		m_columns.clear();
		m_records.clear();
		
		fireTableDataChanged();
	}
}
