package marmot.command;

import marmot.DataSet;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_schema", description="display the schema information of a dataset")
public class RemotePrintSchemaMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="dataset_id", description={"dataset id to display"})
	private String m_dsId;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemotePrintSchemaMain cmd = new RemotePrintSchemaMain();
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
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = m_connector.connect();
	
			DataSet info = marmot.getDataSet(m_dsId);
			
			System.out.println("TYPE         : " + info.getType());
			if ( info.getRecordCount() > 0 ) {
				System.out.println("COUNT        : " + info.getRecordCount());
			}
			else {
				System.out.println("COUNT        : unknown");
			}
			System.out.println("SIZE         : " + UnitUtils.toByteSizeString(info.length()));
			if ( info.hasGeometryColumn() ) {
				System.out.println("GEOMETRY     : " + info.getGeometryColumnInfo().name());
				System.out.println("SRID         : " + info.getGeometryColumnInfo().srid());
			}
			System.out.println("HDFS PATH    : " + info.getHdfsPath());
			System.out.println("COMPRESSION  : " + info.getCompressionCodecName().getOrElse("none"));
			SpatialIndexInfo idxInfo = info.getDefaultSpatialIndexInfo().getOrNull();
			System.out.printf("SPATIAL INDEX: %s%n", (idxInfo != null)
														? idxInfo.getHdfsFilePath() : "none");
			System.out.println("COLUMNS      :");
			info.getRecordSchema().getColumns()
					.stream()
					.forEach(c -> System.out.println("\t" + c));
		}
		catch ( Exception e ) {
			System.err.println("" + e);
		}
	}
}
