package marmot.command.explorer;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import javax.swing.Icon;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class DataSetTree extends JTree {
	private static final long serialVersionUID = 1L;
	
	private final MarmotRuntime m_marmot;
	private final DefaultMutableTreeNode m_root;
	private final BehaviorSubject<Selection> m_subject = BehaviorSubject.create();
	
	private final JMenuItem m_menuItemDelete = new JMenuItem("delete");
	private final JMenuItem m_menuItemCluster = new JMenuItem("cluster");
	private final JMenuItem m_menuItemDeleteCluster = new JMenuItem("delete cluster");
	private final JMenuItem m_menuItemExportShp = new JMenuItem("export (shp)");
	
	class Selection {
		DefaultMutableTreeNode m_node;
		FOption<DataSet> m_dataset;
		FOption<String> m_dir;
		
		private Selection(DefaultMutableTreeNode node) {
			m_node = node;
			
			Object uobj = node.getUserObject();
			if ( uobj instanceof DataSet ) {
				m_dataset = FOption.of((DataSet)uobj);
				m_dir = FOption.empty();
			}
			else {
				m_dataset = FOption.empty();
				m_dir = FOption.of(getParentPath(node));
			}
		}
		
		public boolean isDataSetSelection() {
			return m_dataset.isPresent();
		}
		
		public boolean isDirectorySelection() {
			return m_dir.isPresent();
		}
		
		public FOption<DataSet> getSelectedDataSet() {
			return m_dataset;
		}
		
		@Override
		public String toString() {
			String target = (isDataSetSelection())
							? String.format("dataset:%s", m_dataset.get())
							: String.format("dir:%s", m_dir.get());
			return String.format("tree selection: %s", target);
		}
	}
	
	DataSetTree(MarmotRuntime marmot) {
		m_marmot = marmot;
		m_root = new DefaultMutableTreeNode("/");
		
		initInSwingThread();
		
		addMouseListener(m_rightClickListener);
		setRootVisible(true);
		expandRow(0);
		setCellRenderer(new TreeCellRenderer());
		
		m_menuItemDelete.addActionListener(e -> onDeleteMenuSelected(e));
		m_menuItemCluster.addActionListener(e -> onClusterMenuSelected(e));
		m_menuItemDeleteCluster.addActionListener(e -> onDeleteClusterMenuSelected(e));
		m_menuItemExportShp.addActionListener(e -> onExportToShpSelected(e));
	}
	
	Observable<Selection> observeSelection() {
		return m_subject;
	}

	private void initInSwingThread() {
		TreeModel model = new DefaultTreeModel(m_root);

		for ( String dir: listSubDirs("/") ) {
			m_root.add(new DefaultMutableTreeNode(dir));
		}

		List<DataSet> dataSet = m_marmot.getDataSetAllInDir("/", false);
		for ( DataSet data : dataSet ) {
			m_root.add(new DefaultMutableTreeNode(data));
		}

		TreeSelectionListener treeSelectionListener = new TreeSelectionListener() {
			public void valueChanged(TreeSelectionEvent tse) {
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)tse.getPath()
																		.getLastPathComponent();
				m_subject.onNext(new Selection(node));
			}
		};

		addTreeSelectionListener(treeSelectionListener);
		setModel(model);
		
		m_subject.filter(Selection::isDirectorySelection)
				.subscribe(sel -> updateChildren(sel.m_node, sel.m_dir.get()));
	}

	private String getParentPath(DefaultMutableTreeNode node) {
		return "/" + FStream.of(node.getPath())
							.drop(1)
							.map(TreeNode::toString)
							.join("/");
	}

	private void updateChildren(DefaultMutableTreeNode node, String path) {
		setEnabled(false);

		SwingWorker<Void, Object> worker = new SwingWorker<Void, Object>() {
			@Override
			public Void doInBackground() {
				node.removeAllChildren();
				
				try {
					for ( String dir: listSubDirs(path) ) {
						publish(dir);
					}
					
					FStream.from(m_marmot.getDataSetAllInDir(path, false))
							.sort((p1,p2) -> p1.getId().compareTo(p2.getId()))
							.forEach(this::publish);
				}
				catch ( Exception e ) {
					e.printStackTrace();
				}
				
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
				updateUI();
				expandPath(new TreePath(node.getPath()));
				setEnabled(true);
			}
		};
		worker.execute();
	}
	
	private final MouseListener m_rightClickListener = new MouseAdapter() {
		@Override
		public void mouseClicked(MouseEvent e) {
			if ( SwingUtilities.isRightMouseButton(e) ) {
				int row = getClosestRowForLocation(e.getX(), e.getY());
				setSelectionRow(row);

				Object selected = getLastSelectedPathComponent();
				if ( selected instanceof DefaultMutableTreeNode ) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode)selected;
					
					JPopupMenu menu = createPopupMenu(node);
					if ( menu != null ) {
						menu.show(e.getComponent(), e.getX(), e.getY());
					}
				}
			}
		}
	};
	
	private JPopupMenu createPopupMenu(DefaultMutableTreeNode node) {
		JPopupMenu menu = new JPopupMenu();
		
		Object userObj = node.getUserObject();
		if ( userObj instanceof DataSet ) {
			DataSet ds = (DataSet)userObj;
			
			menu.setLabel("");
			menu.add(m_menuItemDelete);
			if ( ds.hasSpatialIndex() ) {
				menu.add(m_menuItemDeleteCluster);
			}
			else {
				menu.add(m_menuItemCluster);
			}
			menu.add(m_menuItemExportShp);
		}
		else {
			menu.setLabel("");
			menu.add(m_menuItemDelete);
		}
		
		return menu;
	}
	
	private void onDeleteMenuSelected(ActionEvent e) {
		int confirmDelete = JOptionPane.showConfirmDialog(null, "Are you sure?",
										"Please select", JOptionPane.YES_NO_OPTION);
		if ( confirmDelete != JOptionPane.YES_OPTION ) {
			return;
		}
		
		Object obj = getLastSelectedPathComponent();
		if ( obj instanceof DefaultMutableTreeNode ) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)obj;
			Object usrObj = node.getUserObject();
			if ( usrObj instanceof DataSet ) {
				m_marmot.deleteDataSet(((DataSet)usrObj).getId());
				// JOptionPane.showMessageDialog(null, "Delete Item " +
				// ((DataSet)usrObj).getId());
			}
			else {
				m_marmot.deleteDir(getParentPath(node));
				// JOptionPane.showMessageDialog(null, "Delete Item " +
				// getParentPath(node));
			}
			
			SwingUtilities.invokeLater(() -> {
				TreeNode parent = node.getParent();
				if ( parent instanceof DefaultMutableTreeNode ) {
					m_subject.onNext(new Selection((DefaultMutableTreeNode)parent));
//					setSelectionPath(new TreePath(((DefaultMutableTreeNode)parent).getPath()));
				}
			});
		}
	}
	
	private void onClusterMenuSelected(ActionEvent e) {
		Object obj = getLastSelectedPathComponent();
		if ( obj instanceof DefaultMutableTreeNode ) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)obj;
			Object usrObj = node.getUserObject();
			if ( usrObj instanceof DataSet ) {
				DataSet ds = (DataSet)usrObj;
				
				String msg = String.format("Cluster dataset '%s'?", ds.getId());
				int yesno = JOptionPane.showConfirmDialog(null, msg, "Please select",
															JOptionPane.YES_NO_OPTION);
				if ( yesno != JOptionPane.YES_OPTION ) {
					return;
				}

				SwingWorker<Void, Object> worker = new SwingWorker<Void, Object>() {
					@Override
					public Void doInBackground() {
						ds.createSpatialIndex();
						
						return null;
					}

					@Override
					protected void process(List<Object> chunks) { }

					@Override
					protected void done() {
						if ( node.equals(getLastSelectedPathComponent()) ) {
							DefaultMutableTreeNode parent = (DefaultMutableTreeNode)node.getParent();
							setSelectionPath(new TreePath(parent));
							setSelectionPath(new TreePath(node.getPath()));
						}
					}
				};
				worker.execute();
			}
		}
	}
	
	private void onDeleteClusterMenuSelected(ActionEvent e) {
		Object obj = getLastSelectedPathComponent();
		if ( obj instanceof DefaultMutableTreeNode ) {
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)obj;
			Object usrObj = node.getUserObject();
			if ( usrObj instanceof DataSet ) {
				DataSet ds = (DataSet)usrObj;
				
				String msg = String.format("Delete the cluster of datset '%s'?", ds.getId());
				int yesno = JOptionPane.showConfirmDialog(null, msg, "Please select",
															JOptionPane.YES_NO_OPTION);
				if ( yesno != JOptionPane.YES_OPTION ) {
					return;
				}
				ds.deleteSpatialIndex();

				SwingUtilities.invokeLater(() -> {
					DefaultMutableTreeNode parent = (DefaultMutableTreeNode)node.getParent();
					setSelectionPath(new TreePath(parent));
					setSelectionPath(new TreePath(node.getPath()));
				});
			}
		}
	}
	
	private void onExportToShpSelected(ActionEvent e) {
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

			Object obj = getLastSelectedPathComponent();
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
							Plan plan = Plan.builder("sample")
											.load(ds.getId())
											.sample(ratio)
											.build();
							rset = m_marmot.executeToRecordSet(plan);
						}
						else {
							rset = ds.read();
						}
						rset = rset.asAutoCloseable();

						Charset cs = Charset.forName(charSet.getText());
						ExportShapefileParameters params = ExportShapefileParameters.create()
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
	}
	
	private List<String> listSubDirs(String path) {
		List<String> subDirs = m_marmot.getSubDirAll(path, false);
		subDirs.sort((s1,s2) -> s1.compareTo(s2));
		if ( subDirs.remove("tmp") ) {
			subDirs.add("tmp");
		}
		
		return subDirs;
	}

	private static class SimpleSubscriber implements Observer<Long> {
		private final File m_shpFile;
		private final FOption<Long> m_total;
		private long m_count;
		
		public SimpleSubscriber(File shpFile, long total) {
			m_shpFile = shpFile;
			m_total = FOption.of(total);
			m_count = -1;
		}
		
		public SimpleSubscriber(File shpFile) {
			m_shpFile = shpFile;
			m_total = FOption.empty();
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
}
