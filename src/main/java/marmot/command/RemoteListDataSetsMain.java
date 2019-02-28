package marmot.command;

import java.util.List;

import marmot.DataSet;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_dir", description="display the names of datasets")
public class RemoteListDataSetsMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="path", arity = "0..*", description={"directory path to display from"})
	private String m_start;

	@Option(names={"-r"}, description="list all descendant datasets")
	private boolean m_recursive;

	@Option(names={"-l"}, description="list in detail")
	private boolean m_details;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteListDataSetsMain cmd = new RemoteListDataSetsMain();
		CommandLine commandLine = new CommandLine(cmd);
		
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void run() {
		try {
			PBMarmotClient marmot = m_connector.connect();
			
			List<DataSet> dsList;
			if ( m_start != null ) {
				dsList = marmot.getDataSetAllInDir(m_start, m_recursive);
			}
			else {
				dsList = marmot.getDataSetAll();
			}
			
			for ( DataSet ds: dsList ) {
				System.out.print(ds.getId());
				
				if ( m_details ) {
					System.out.printf(" %s %s", ds.getType(), ds.getHdfsPath());
					if ( ds.hasGeometryColumn() ) {
						System.out.printf(" %s", ds.getGeometryColumnInfo());
						
						SpatialIndexInfo idxInfo = ds.getDefaultSpatialIndexInfoOrNull();
						if ( idxInfo != null ) {
							System.out.printf("(clustered)");
						}
					}
				}
				System.out.println();
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
}
