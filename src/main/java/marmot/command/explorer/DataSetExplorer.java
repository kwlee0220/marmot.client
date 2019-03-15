package marmot.command.explorer;


import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import io.reactivex.Observable;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.command.MarmotClientCommands;
import marmot.command.explorer.DataSetTree.Selection;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineException;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetExplorer extends JFrame {
	private static final long serialVersionUID = 5835045489006027819L;

	private final MarmotRuntime m_marmot;
	private DataSetTree m_tree;
	private DataSetTableModel m_dsTable;
	private DataSetInfoPanel m_dsInfoPanel;
	private DefaultMutableTreeNode rootNode;

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
						DataSetExplorer frame = new DataSetExplorer(marmot);
						frame.setVisible(true);
						frame.init();
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

	private void init() {
		
	}

	/**
	 * Create the frame.
	 */
	public DataSetExplorer(MarmotRuntime marmot) {
		m_marmot = marmot;
		
		setMinimumSize(new Dimension(1500, 695));
		setLocationByPlatform(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		JPanel contentPane = new JPanel(new BorderLayout(3, 3));
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);

		JPanel panel = new JPanel(new BorderLayout(3, 3));

		m_tree = new DataSetTree(m_marmot);
		JScrollPane scrollPane = new JScrollPane(m_tree);
		scrollPane.setPreferredSize(new Dimension(200, 324));
		JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, scrollPane, panel);
		contentPane.add(splitPane, BorderLayout.CENTER);
		
		Observable<Selection> channel = m_tree.observeSelection();

		m_dsInfoPanel = new DataSetInfoPanel();
		panel.add(m_dsInfoPanel, BorderLayout.NORTH);

		m_dsTable = new DataSetTableModel(m_marmot);
		channel.subscribe(sel -> {
			try {
				if ( sel.isDataSetSelection() ) {
					DataSet ds = sel.m_dataset.getUnchecked();
					
					m_dsInfoPanel.update(ds);
					m_dsTable.update(ds);
				}
				else {
					m_dsInfoPanel.update(sel.m_dir.getUnchecked());
					m_dsTable.clear();
				}
			}
			catch ( Exception ignored ) { }
		});
		
		JTable table = new JTable();
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setShowVerticalLines(false);
		table.setAutoCreateRowSorter(true);
		table.setModel(m_dsTable);
		
		JScrollPane scrollPane_1 = new JScrollPane(table);
		panel.add(scrollPane_1, BorderLayout.CENTER);

		JPanel panel_1 = new JPanel(new BorderLayout(3, 3));
		panel.add(panel_1, BorderLayout.SOUTH);

		JToolBar toolBar = new JToolBar();
		toolBar.setFloatable(false);
		panel_1.add(toolBar, BorderLayout.NORTH);
	}
}
