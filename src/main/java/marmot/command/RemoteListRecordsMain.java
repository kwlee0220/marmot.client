package marmot.command;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import marmot.Column;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
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
@Command(name="mc_list", description="display the records of a dataset")
public class RemoteListRecordsMain implements Runnable {
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="dataset_id", description={"dataset id to display"})
	private String m_dsId;

	@Option(names={"-project"}, paramLabel="column_list", description="selected columns (optional)")
	private String m_cols = null;
	
	@Option(names={"-limit"}, paramLabel="count", description="limit count (optional)")
	private int m_limit = -1;

	@Option(names={"-csv"}, description="display csv format")
	private boolean m_asCsv;

	@Option(names={"-delim"}, paramLabel="character", description="csv delimiter (default: ',')")
	private String m_delim = ",";

	@Option(names={"-geom"}, description="display geometry columns")
	private boolean m_displayGeom;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		RemoteListRecordsMain cmd = new RemoteListRecordsMain();
		CommandLine commandLine = new CommandLine(cmd);
		commandLine.parse(args);
		
		if ( commandLine.isUsageHelpRequested() ) {
			commandLine.usage(System.out, Ansi.OFF);
		}
		else {
			cmd.run();
		}
	}

	@Override
	public void run() {
		try {
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = m_connector.connect();
			
			PlanBuilder builder = marmot.planBuilder("list_records")
										.load(m_dsId);
			if ( m_limit > 0 ) {
				builder = builder.take(m_limit);
			}
			if ( m_cols != null ) {
				builder = builder.project(m_cols);
			}
		
			if ( !m_displayGeom ) {
				Plan tmp = builder.build();
				RecordSchema schema = marmot.getOutputRecordSchema(tmp);
				String cols = schema.columnFStream()
									.filter(col -> col.type().isGeometryType())
									.map(Column::name)
									.join(",");
				if ( cols.length() > 0 ) {
					builder.project(String.format("*-{%s}", cols));
				}
			}
			
			try ( RecordSet rset = marmot.executeLocally(builder.build()) ) {
				Record record = DefaultRecord.of(rset.getRecordSchema());
				while ( rset.next(record) ) {
					Map<String,Object> values = record.toMap();
					
					if ( m_asCsv ) {
						System.out.println(toCsv(values.values(), m_delim));
					}
					else {
						System.out.println(values);
					}
				}
			}
		}
		catch ( Exception e ) {
			System.err.println("" + e);
		}
	}
	
	private static String toCsv(Collection<?> values, String delim) {
		return values.stream()
					.map(o -> {
						String str = ""+o;
						if ( str.contains(" ") || str.contains(delim) ) {
							str = "\"" + str + "\"";
						}
						return str;
					})
					.collect(Collectors.joining(delim));
	}
}
