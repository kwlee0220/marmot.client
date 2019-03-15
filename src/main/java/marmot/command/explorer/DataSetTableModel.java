package marmot.command.explorer;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import marmot.Column;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;


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
		PlanBuilder builder = m_marmot.planBuilder("list a dataset")
									.load(ds.getId());
		if ( ds.hasGeometryColumn() ) {
			String geomCol = ds.getGeometryColumn();
			String prjCol = String.format("*-{%s}", geomCol);
			builder = builder.project(prjCol);

			m_columns = ds.getRecordSchema()
							.streamColumns()
							.filter(col -> !col.name().equals(geomCol))
							.toList();
		}
		else {
			m_columns = ds.getRecordSchema()
							.streamColumns().toList();
		}
		Plan plan = builder.take(30).build();
		m_records = m_marmot.executeLocally(plan).toList();
		
		fireTableStructureChanged();
	}

	void clear() {
		m_columns.clear();
		m_records.clear();
		
		fireTableDataChanged();
	}
}
