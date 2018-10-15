package marmot.command;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.Column;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListRecordsMain {
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_list ");
		parser.addArgumentName("dataset|file_path");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: $MARMOT_HOST)", false);
		parser.addArgOption("port", "number", "marmot server port (default: $MARMOT_PORT)", false);
		parser.addArgOption("limit", "count", "limit count (optional)", false);
		parser.addArgOption("project", "cols", "selected columns (optional)", false);
		parser.addOption("g", "displaying geometry (optional)", false);
		parser.addOption("csv", "display csv format", false);
		parser.addArgOption("delim", "character", "csv delimiter (default: ',')", false);
		parser.addOption("file", "load from raw MarmotFile", false);
		parser.addOption("h", "print usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("h") ) {
			cl.exitWithUsage(0);
		}
		
		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		String dsId = cl.getArgument(0);
		long limit = cl.getOptionLong("limit").getOrElse(-1L);
		String project = cl.getOptionString("project").getOrNull();
		boolean displayGeom = cl.hasOption("g");
		boolean csv = cl.hasOption("csv");
		String delim = cl.getOptionString("delim").getOrElse(",");
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		PlanBuilder builder = marmot.planBuilder("list_records");
		
		builder = cl.hasOption("file") ? builder.loadMarmotFile(dsId)
										: builder.load(dsId);
		if ( limit > 0 ) {
			builder = builder.take(limit);
		}
		
		if ( project != null ) {
			builder = builder.project(project);
		}
		
		try {
			if ( !displayGeom ) {
				Plan tmp = builder.build();
				RecordSchema schema = marmot.getOutputRecordSchema(tmp);
				String cols = schema.stream()
									.filter(col -> col.type().isGeometryType())
									.map(Column::name)
									.collect(Collectors.joining(","));
				if ( cols.length() > 0 ) {
					builder.project(String.format("*-{%s}", cols));
				}
			}
			
			try ( RecordSet rset = marmot.executeLocally(builder.build()) ) {
				Record record = DefaultRecord.of(rset.getRecordSchema());
				while ( rset.next(record) ) {
					Map<String,Object> values = record.toMap();
					
					if ( csv ) {
						System.out.println(toCsv(values.values(), delim));
					}
					else {
						System.out.println(values);
					}
				}
			}
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(-1);
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
