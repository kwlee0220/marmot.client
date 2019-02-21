package marmot.command;


import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.JTree;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.AbstractTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.vavr.control.Option;
import marmot.Column;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.RecordSets;
import utils.CommandLine;
import utils.CommandLineException;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetExplorer extends JFrame {
	private static final long serialVersionUID = 5835045489006027819L;

	private JTree tree;
	private DefaultTreeModel treeModel;
	private JTable table;
	private DetailTableModel tableModel;
	private PBMarmotClient marmot;
	private JLabel lblDirName;
	private JLabel lblHdfsPath;
	private JLabel lblCount;
	private JLabel lblSRiD;
	private JLabel lblGeometryColumn;
	private JLabel lblType;
	private JLabel lblFileID;
	private DefaultMutableTreeNode rootNode;
	private JPopupMenu popupMenu;

	public static void main(String[] args) throws IOException {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_explorer ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addOption("help", "show usage", false);
		
		CommandLine cl = null;
		try {
			cl = parser.parseArgs(args);
			if ( cl.hasOption("help") ) {
				cl.exitWithUsage(0);
			}

			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);

			EventQueue.invokeLater(new Runnable() {
				public void run() {
					try {
						DataSetExplorer frame = new DataSetExplorer();
						frame.setVisible(true);
						frame.init(marmot);
					}
					catch ( Exception e ) {
						e.printStackTrace();
					}
				}
			});
		}
		catch ( ParseException | CommandLineException e ) {
			System.err.println(e);
			if ( cl != null ) {
				cl.exitWithUsage(-1);
			}
		}
	}

	private void init(PBMarmotClient client) {
		this.marmot = client;

		// the File tree
		rootNode = new DefaultMutableTreeNode("");
		treeModel = new DefaultTreeModel(rootNode);

		List<String> subDir = marmot.getSubDirAll("/", false);
		for ( String dir : subDir ) {
			rootNode.add(new DefaultMutableTreeNode(dir));
		}

		List<DataSet> dataSet = marmot.getDataSetAllInDir("/", false);
		for ( DataSet data : dataSet ) {
			rootNode.add(new DefaultMutableTreeNode(data));
		}

		TreeSelectionListener treeSelectionListener = new TreeSelectionListener() {
			public void valueChanged(TreeSelectionEvent tse) {
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)tse.getPath()
						.getLastPathComponent();

				Object usrObj = node.getUserObject();
				if ( usrObj instanceof DataSet ) {
					SwingUtilities.invokeLater(() -> {
						clearFileLabel();
						tableModel.clear();

						DataSet data = (DataSet)usrObj;
						setFileLabel(data);

						List<Column> columns = data.getRecordSchema().getColumns().stream()
								.collect(Collectors.toList());
						List<Record> records = data.read().stream().limit(10)
								.collect(Collectors.toList());
						tableModel.setRecordSet(columns, records);

					});
				}
				else {
					showChildren(node);
				}
			}
		};

		tree.addTreeSelectionListener(treeSelectionListener);
		tree.setModel(treeModel);

	}

	private void showChildren(final DefaultMutableTreeNode node) {
		tree.setEnabled(false);

		SwingWorker<Void, Object> worker = new SwingWorker<Void, Object>() {
			@Override
			public Void doInBackground() {
				clearFileLabel();
				tableModel.clear();
				tableModel.fireTableDataChanged();
				node.removeAllChildren();
				String path = getParentPath(node);
				List<String> subDir = marmot.getSubDirAll(path, false);
				for ( String dir : subDir ) {
					publish(dir);
				}

				List<DataSet> dataSet = marmot.getDataSetAllInDir(path, false);
				publish(dataSet.toArray());
				return null;
			}

			@Override
			protected void process(List<Object> chunks) {
				for ( Object child : chunks ) {
					node.add(new DefaultMutableTreeNode(child));
				}
			}

			@Override
			protected void done() {
				tree.updateUI();
				tree.expandPath(new TreePath(node.getPath()));
				tree.setEnabled(true);
			}
		};
		worker.execute();
	}

	private String getParentPath(DefaultMutableTreeNode node) {
		if ( node == rootNode )
			return "/";

		return Arrays.stream(node.getPath()).map(TreeNode::toString)
				.collect(Collectors.joining("/"));
	}

	private void setFileLabel(DataSet ds) {
		lblDirName.setText(ds.getDirName());
		lblHdfsPath.setText(ds.getHdfsPath());
		lblCount.setText(Long.toString(ds.getRecordCount()));
		if ( ds.hasGeometryColumn() ) {
			GeometryColumnInfo gcinfo = ds.getGeometryColumnInfo();
			lblSRiD.setText(gcinfo.srid());
			lblGeometryColumn.setText(gcinfo.name() + "(*)");
		}
		else {
			lblSRiD.setText("none");
			lblGeometryColumn.setText("none");
		}
		lblType.setText(ds.getType().toString());
		lblFileID.setText(ds.getId());
	}

	private void clearFileLabel() {
		lblDirName.setText("");
		lblHdfsPath.setText("");
		lblCount.setText("");
		lblSRiD.setText("");
		lblGeometryColumn.setText("");
		lblType.setText("");
		lblFileID.setText("");
	}

	/**
	 * Create the frame.
	 */
	public DataSetExplorer() {
		setMinimumSize(new Dimension(714, 512));
		setLocationByPlatform(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		JPanel contentPane = new JPanel(new BorderLayout(3, 3));
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));

		setContentPane(contentPane);

		JPanel panel = new JPanel(new BorderLayout(3, 3));

		tree = new JTree();
		tree.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if ( SwingUtilities.isRightMouseButton(e) ) {
					int row = tree.getClosestRowForLocation(e.getX(), e.getY());
					tree.setSelectionRow(row);
					popupMenu.show(e.getComponent(), e.getX(), e.getY());
				}
			}
		});
		tree.setRootVisible(true);
		tree.expandRow(0);
		tree.setCellRenderer(new TreeCellRenderer());

		JScrollPane scrollPane = new JScrollPane(tree);
		scrollPane.setPreferredSize(new Dimension(200, 324));

		JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, scrollPane, panel);

		tableModel = new DetailTableModel();

		table = new JTable();
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setShowVerticalLines(false);
		table.setAutoCreateRowSorter(true);
		table.setModel(tableModel);

		JScrollPane scrollPane_1 = new JScrollPane(table);
		panel.add(scrollPane_1, BorderLayout.CENTER);

		JPanel panel_1 = new JPanel(new BorderLayout(3, 3));
		panel.add(panel_1, BorderLayout.SOUTH);

		JToolBar toolBar = new JToolBar();
		toolBar.setFloatable(false);
		panel_1.add(toolBar, BorderLayout.NORTH);

		JButton btnNewButton = new JButton("New button");
		toolBar.add(btnNewButton);

		JButton btnNewButton_1 = new JButton("New button");
		toolBar.add(btnNewButton_1);

		JPanel panel_2 = new JPanel();
		panel.add(panel_2, BorderLayout.NORTH);
		GridBagLayout gbl_panel_2 = new GridBagLayout();
		gbl_panel_2.columnWidths = new int[] { 0, 0, 0 };
		gbl_panel_2.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_panel_2.columnWeights = new double[] { 0.0, 0.0, Double.MIN_VALUE };
		gbl_panel_2.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
				Double.MIN_VALUE };
		panel_2.setLayout(gbl_panel_2);

		JLabel lblNewLabel = new JLabel("FileID");
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = 0;
		gbc_lblNewLabel.gridy = 0;
		panel_2.add(lblNewLabel, gbc_lblNewLabel);

		lblFileID = new JLabel("");
		GridBagConstraints gbc_lblFileID = new GridBagConstraints();
		gbc_lblFileID.anchor = GridBagConstraints.WEST;
		gbc_lblFileID.insets = new Insets(0, 0, 5, 0);
		gbc_lblFileID.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblFileID.gridx = 1;
		gbc_lblFileID.gridy = 0;
		panel_2.add(lblFileID, gbc_lblFileID);

		JLabel lblNewLabel_2 = new JLabel("dirName");
		GridBagConstraints gbc_lblNewLabel_2 = new GridBagConstraints();
		gbc_lblNewLabel_2.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_2.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_2.gridx = 0;
		gbc_lblNewLabel_2.gridy = 1;
		panel_2.add(lblNewLabel_2, gbc_lblNewLabel_2);

		lblDirName = new JLabel("");
		GridBagConstraints gbc_lblDirName = new GridBagConstraints();
		gbc_lblDirName.anchor = GridBagConstraints.WEST;
		gbc_lblDirName.insets = new Insets(0, 0, 5, 0);
		gbc_lblDirName.gridx = 1;
		gbc_lblDirName.gridy = 1;
		panel_2.add(lblDirName, gbc_lblDirName);

		JLabel lblNewLabel_4 = new JLabel("Type");
		GridBagConstraints gbc_lblNewLabel_4 = new GridBagConstraints();
		gbc_lblNewLabel_4.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_4.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_4.gridx = 0;
		gbc_lblNewLabel_4.gridy = 2;
		panel_2.add(lblNewLabel_4, gbc_lblNewLabel_4);

		lblType = new JLabel("");
		GridBagConstraints gbc_lblType = new GridBagConstraints();
		gbc_lblType.anchor = GridBagConstraints.WEST;
		gbc_lblType.insets = new Insets(0, 0, 5, 0);
		gbc_lblType.gridx = 1;
		gbc_lblType.gridy = 2;
		panel_2.add(lblType, gbc_lblType);

		JLabel lblNewLabel_6 = new JLabel("GeometryColumn");
		GridBagConstraints gbc_lblNewLabel_6 = new GridBagConstraints();
		gbc_lblNewLabel_6.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_6.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_6.gridx = 0;
		gbc_lblNewLabel_6.gridy = 3;
		panel_2.add(lblNewLabel_6, gbc_lblNewLabel_6);

		lblGeometryColumn = new JLabel("");
		GridBagConstraints gbc_lblGeometryColumn = new GridBagConstraints();
		gbc_lblGeometryColumn.anchor = GridBagConstraints.WEST;
		gbc_lblGeometryColumn.insets = new Insets(0, 0, 5, 0);
		gbc_lblGeometryColumn.gridx = 1;
		gbc_lblGeometryColumn.gridy = 3;
		panel_2.add(lblGeometryColumn, gbc_lblGeometryColumn);

		JLabel lblNewLabel_8 = new JLabel("SRiD");
		GridBagConstraints gbc_lblNewLabel_8 = new GridBagConstraints();
		gbc_lblNewLabel_8.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_8.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_8.gridx = 0;
		gbc_lblNewLabel_8.gridy = 4;
		panel_2.add(lblNewLabel_8, gbc_lblNewLabel_8);

		lblSRiD = new JLabel("");
		GridBagConstraints gbc_lblSRiD = new GridBagConstraints();
		gbc_lblSRiD.anchor = GridBagConstraints.WEST;
		gbc_lblSRiD.insets = new Insets(0, 0, 5, 0);
		gbc_lblSRiD.gridx = 1;
		gbc_lblSRiD.gridy = 4;
		panel_2.add(lblSRiD, gbc_lblSRiD);

		JLabel lblNewLabel_10 = new JLabel("Count");
		GridBagConstraints gbc_lblNewLabel_10 = new GridBagConstraints();
		gbc_lblNewLabel_10.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_10.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_10.gridx = 0;
		gbc_lblNewLabel_10.gridy = 5;
		panel_2.add(lblNewLabel_10, gbc_lblNewLabel_10);

		lblCount = new JLabel("");
		GridBagConstraints gbc_lblCount = new GridBagConstraints();
		gbc_lblCount.anchor = GridBagConstraints.WEST;
		gbc_lblCount.insets = new Insets(0, 0, 5, 0);
		gbc_lblCount.gridx = 1;
		gbc_lblCount.gridy = 5;
		panel_2.add(lblCount, gbc_lblCount);

		JLabel lblNewLabel_12 = new JLabel("HdfsPath");
		GridBagConstraints gbc_lblNewLabel_12 = new GridBagConstraints();
		gbc_lblNewLabel_12.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_12.insets = new Insets(0, 0, 0, 5);
		gbc_lblNewLabel_12.gridx = 0;
		gbc_lblNewLabel_12.gridy = 6;
		panel_2.add(lblNewLabel_12, gbc_lblNewLabel_12);

		lblHdfsPath = new JLabel("");
		GridBagConstraints gbc_lblHdfsPath = new GridBagConstraints();
		gbc_lblHdfsPath.anchor = GridBagConstraints.WEST;
		gbc_lblHdfsPath.gridx = 1;
		gbc_lblHdfsPath.gridy = 6;
		panel_2.add(lblHdfsPath, gbc_lblHdfsPath);
		contentPane.add(splitPane, BorderLayout.CENTER);

		JMenuItem popupItemDelete = new JMenuItem("Delete");
		popupItemDelete.addActionListener((ActionEvent e) -> {
			int confirmDelete = JOptionPane.showConfirmDialog(null, "Are you sure?",
					"Please select", JOptionPane.YES_NO_OPTION);
			if ( confirmDelete != JOptionPane.YES_OPTION ) {
				return;
			}
			Object obj = tree.getLastSelectedPathComponent();
			if ( obj instanceof DefaultMutableTreeNode ) {
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)obj;
				Object usrObj = node.getUserObject();
				if ( usrObj instanceof DataSet ) {
					marmot.deleteDataSet(((DataSet)usrObj).getId());
					// JOptionPane.showMessageDialog(null, "Delete Item " +
					// ((DataSet)usrObj).getId());
				}
				else {
					marmot.deleteDir(getParentPath(node));
					// JOptionPane.showMessageDialog(null, "Delete Item " +
					// getParentPath(node));
				}
				SwingUtilities.invokeLater(() -> {
					TreeNode parent = node.getParent();
					if ( parent instanceof DefaultMutableTreeNode ) {
						tree.setSelectionPath(
								new TreePath(((DefaultMutableTreeNode)parent).getPath()));
					}
				});
			}
		});
		JMenuItem popupItemNew = new JMenuItem("Export (SHP)");
		popupItemNew.addActionListener((ActionEvent e) -> {
			JFileChooser fileChooser = new JFileChooser();
			fileChooser.setDialogTitle("Specify a file to save");
			fileChooser.setFileFilter(new FileNameExtensionFilter("Shape File", "shp"));

			JPanel accessoryPanel = new JPanel();
			GridBagLayout gbl_textPanel = new GridBagLayout();
			gbl_textPanel.columnWidths = new int[] { 0, 0, 0 };
			gbl_textPanel.rowHeights = new int[] { 0, 0, 0 };
			gbl_textPanel.columnWeights = new double[] { 0.0, 1.0, Double.MIN_VALUE };
			gbl_textPanel.rowWeights = new double[] { 0.0, 0.0, Double.MIN_VALUE };
			accessoryPanel.setLayout(gbl_textPanel);

			JLabel charsetLabel = new JLabel("Charset");
			GridBagConstraints gbc_charsetLabel = new GridBagConstraints();
			gbc_charsetLabel.anchor = GridBagConstraints.WEST;
			gbc_charsetLabel.insets = new Insets(0, 0, 5, 5);
			gbc_charsetLabel.gridx = 0;
			gbc_charsetLabel.gridy = 0;
			accessoryPanel.add(charsetLabel, gbc_charsetLabel);

			JTextField charSet = new JTextField("euc-kr");
			GridBagConstraints gbc_charSet = new GridBagConstraints();
			gbc_charSet.insets = new Insets(0, 0, 5, 0);
			gbc_charSet.fill = GridBagConstraints.HORIZONTAL;
			gbc_charSet.gridx = 1;
			gbc_charSet.gridy = 0;
			accessoryPanel.add(charSet, gbc_charSet);
			charSet.setColumns(10);

			JLabel sampleLabel = new JLabel("Sample (0<x≤1)");
			GridBagConstraints gbc_sampleLabel = new GridBagConstraints();
			gbc_sampleLabel.anchor = GridBagConstraints.WEST;
			gbc_sampleLabel.insets = new Insets(0, 0, 0, 5);
			gbc_sampleLabel.gridx = 0;
			gbc_sampleLabel.gridy = 1;
			accessoryPanel.add(sampleLabel, gbc_sampleLabel);

			JTextField sampleText = new JTextField("1");
			GridBagConstraints gbc_sampleText = new GridBagConstraints();
			gbc_sampleText.fill = GridBagConstraints.HORIZONTAL;
			gbc_sampleText.gridx = 1;
			gbc_sampleText.gridy = 1;
			accessoryPanel.add(sampleText, gbc_sampleText);
			sampleText.setColumns(10);

			JPanel panel_checkBox = new JPanel();
			GridBagConstraints gbc_panel_checkBox = new GridBagConstraints();
			gbc_panel_checkBox.gridwidth = 2;
			gbc_panel_checkBox.insets = new Insets(0, 0, 0, 5);
			gbc_panel_checkBox.fill = GridBagConstraints.BOTH;
			gbc_panel_checkBox.gridx = 0;
			gbc_panel_checkBox.gridy = 2;
			accessoryPanel.add(panel_checkBox, gbc_panel_checkBox);

			JCheckBox seqCheck = new JCheckBox("use_seq_column");
			panel_checkBox.add(seqCheck);

			fileChooser.setAccessory(accessoryPanel);

			int userSelection = fileChooser.showSaveDialog(this);

			if ( userSelection == JFileChooser.APPROVE_OPTION ) {
				File fileToSave = fileChooser.getSelectedFile();
				String fname = fileToSave.getAbsolutePath();

				if ( !fname.endsWith(".shp") ) {
					fileToSave = new File(fname + ".shp");
				}

//				if ( fileToSave.exists() ) {
//					JOptionPane.showMessageDialog(null, "file already exists");
//					return;
//				}

				Object obj = tree.getLastSelectedPathComponent();
				if ( obj instanceof DefaultMutableTreeNode ) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode)obj;
					Object usrObj = node.getUserObject();
					if ( usrObj instanceof DataSet ) {
						DataSet ds = (DataSet)usrObj;
						String srid = ds.getGeometryColumnInfo().srid();
						long total = ds.getRecordCount();
						
						RecordSet rset;
						try {
							double ratio = Double.parseDouble(sampleText.getText());
							if ( ratio > 0 && ratio < 1 ) {
								Plan plan = marmot.planBuilder("sample")
												.load(ds.getId())
												.sample(ratio)
												.build();
								rset = marmot.executeToRecordSet(plan);
							}
							else {
								rset = ds.read();
							}
							rset = RecordSets.autoClose(rset);

							Charset cs = Charset.forName(charSet.getText());
							ShapefileParameters params = ShapefileParameters.create()
															.charset(cs);
							
							ExportRecordSetAsShapefile export
								= new ExportRecordSetAsShapefile(rset, srid,
																fileToSave.getAbsolutePath(),
																params);
							export.setForce(true);
							export.start()
									.getProgressObservable()
									.subscribe(new SimpleSubscriber(fileToSave, total));
						}
						catch ( Exception ex ) {
							JOptionPane.showMessageDialog(null, ex.getMessage());
						}
					}
				}
				System.out.println("Save as file: " + fileToSave.getAbsolutePath());
			}
		});
		popupMenu = new JPopupMenu();
		popupMenu.setLabel("");
		popupMenu.add(popupItemDelete);
		popupMenu.add(popupItemNew);
	}

	class DetailTableModel extends AbstractTableModel {
		private static final long serialVersionUID = 1L;

		private List<Column> columns = new ArrayList<>();

		List<Record> dataSet = new ArrayList<>();

		@Override
		public int getColumnCount() {
			return columns.size();
		}

		@Override
		public String getColumnName(int i) {
			return columns.get(i).name();
		}

		@Override
		public int getRowCount() {
			return dataSet.size();
		}

		@Override
		public Object getValueAt(int rowIndex, int columnIndex) {
			return dataSet.get(rowIndex).get(columnIndex);
		}

		public void setRecordSet(List<Column> columns, List<Record> s) {
			clear();
			this.columns.addAll(columns);
			this.dataSet.addAll(s);
			fireTableStructureChanged();
			// table.getTableHeader().repaint();
			// fireTableDataChanged();
		}

		public void clear() {
			this.columns.clear();
			this.dataSet.clear();
		}
	}

	/** A TreeCellRenderer for a File. */
	class TreeCellRenderer extends DefaultTreeCellRenderer {
		private static final long serialVersionUID = -2775494607234790478L;

		private final Icon fileIcon = UIManager.getIcon("FileView.fileIcon");

		private final Icon directoryIcon = UIManager.getIcon("FileView.directoryIcon");

		private JLabel label;

		TreeCellRenderer() {
			label = new JLabel();
			label.setOpaque(true);
		}

		@SuppressWarnings("hiding")
		@Override
		public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected,
				boolean expanded, boolean leaf, int row, boolean hasFocus) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
			Object usrObj = node.getUserObject();
			if ( usrObj instanceof DataSet ) {
				DataSet data = (DataSet)usrObj;
				label.setIcon(fileIcon);
				int idx = data.getId().lastIndexOf("/") + 1;
				label.setText(data.getId().substring(idx));
			}
			else {
				label.setIcon(directoryIcon);
				label.setText(usrObj.toString());
			}

			if ( selected ) {
				label.setBackground(backgroundSelectionColor);
				label.setForeground(textSelectionColor);
			}
			else {
				label.setBackground(backgroundNonSelectionColor);
				label.setForeground(textNonSelectionColor);
			}

			return label;
		}
	}

	private static class SimpleSubscriber implements Observer<Long> {
		private final File m_shpFile;
		private final Option<Long> m_total;
		private long m_count;
		
		public SimpleSubscriber(File shpFile, long total) {
			m_shpFile = shpFile;
			m_total = Option.some(total);
			m_count = -1;
		}
		
		public SimpleSubscriber(File shpFile) {
			m_shpFile = shpFile;
			m_total = Option.none();
			m_count = -1;
		}
		
		public long getCount() {
			return m_count;
		}

		@Override
		public void onSubscribe(Disposable d) {}
		
		@Override
		public void onNext(Long count) {
			m_count = count;
			if ( m_count == 0 ) {
				System.out.printf("start to export Shapefile: file=%s%n",
									m_shpFile.getAbsolutePath());
			}
			else {
				String ratioStr = m_total.map(total -> (double)m_count / total * 100)
										.map(r -> String.format("%4.1f%%", r))
										.getOrElse("unknown");
				System.out.printf("exporting... count=%8d, (%s)%n", m_count, ratioStr);
			}
		}
		
		@Override
		public void onComplete() {
			System.out.printf("done: total=%d%n", m_count);
		}

		@Override
		public void onError(Throwable e) {
			System.out.printf("fails to export, cause=%s", e);
		}
	}
}
