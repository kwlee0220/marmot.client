package marmot.command.explorer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JLabel;
import javax.swing.JPanel;

import marmot.Column;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class DataSetInfoPanel extends JPanel {
	private static final long serialVersionUID = 1L;
	
	private final JLabel m_id;
	private final JLabel m_type;
	private final JLabel m_geomCol;
	private final JLabel m_srid;
	private final JLabel m_count;
	private final JLabel m_size;
	private final JLabel m_hdfsPath;
	private final JLabel m_blockSize;
	
	DataSetInfoPanel() {
		GridBagLayout layout = new GridBagLayout();
		layout.columnWidths = new int[] { 0, 0, 0 };
		layout.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
		layout.columnWeights = new double[] { 0.0, 0.0, Double.MIN_VALUE };
		layout.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
				Double.MIN_VALUE };
		setLayout(layout);
		
		m_id = addRow("Id:", 0);
		m_type = addRow("Type:", 1);
		m_geomCol = addRow("Geometry:", 2);
		m_srid = addRow("SRID:", 3);
		m_count = addRow("Count:", 4);
		m_size = addRow("Size:", 5);
		m_hdfsPath = addRow("HDFS Path:", 6);
		m_blockSize = addRow("Block-size:", 7);
	}

	void update(DataSet ds) {
		m_id.setText(ds.getId());
		m_type.setText(ds.getType().toString());
		
		if ( ds.hasGeometryColumn() ) {
			GeometryColumnInfo gcinfo = ds.getGeometryColumnInfo();
			m_srid.setText(gcinfo.srid());
			
			Column geomCol = ds.getRecordSchema().getColumn(gcinfo.name());
			String clustered = ds.isSpatiallyClustered() ? ", CLUSTERED" : "";
			String geomStr = String.format("'%s' (%s)%s", gcinfo.name(), geomCol.type(), clustered);
			m_geomCol.setText(geomStr);
		}
		else {
			m_srid.setText("none");
			m_geomCol.setText("none");
		}

		m_count.setText(String.format("%,d", ds.getRecordCount()));
		m_size.setText(UnitUtils.toByteSizeString(ds.length()));
		m_hdfsPath.setText(ds.getHdfsPath());
		m_blockSize.setText(UnitUtils.toByteSizeString(ds.getBlockSize()));
	}

	void update(String dirPath) {
		m_id.setText(dirPath);
		m_type.setText("Directory");
		m_geomCol.setText("");
		m_srid.setText("");
		m_count.setText("");
		m_size.setText("");
		m_hdfsPath.setText("");
		m_blockSize.setText("");
	}

	void clear() {
		m_id.setText("");
		m_type.setText("");
		m_geomCol.setText("");
		m_srid.setText("");
		m_count.setText("");
		m_size.setText("");
		m_hdfsPath.setText("");
		m_blockSize.setText("");
	}
	
	private JLabel addRow(String title, int row) {
		GridBagConstraints constraints;
		
		JLabel label = new JLabel(title);
		constraints = new GridBagConstraints();
		constraints.anchor = GridBagConstraints.WEST;
		constraints.insets = new Insets(0, 0, 5, 5);
		constraints.gridx = 0;
		constraints.gridy = row;
		add(label, constraints);

		JLabel text = new JLabel("");
		constraints = new GridBagConstraints();
		constraints.anchor = GridBagConstraints.WEST;
		constraints.insets = new Insets(0, 0, 5, 0);
		constraints.fill = GridBagConstraints.HORIZONTAL;
		constraints.gridx = 1;
		constraints.gridy = row;
		add(text, constraints);
		
		return text;
	}
}
