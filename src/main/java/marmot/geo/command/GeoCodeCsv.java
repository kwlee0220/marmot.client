package marmot.geo.command;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;

import marmot.support.GeoCoder;
import marmot.support.GoogleGeoCoder;
import marmot.support.NaverGeoCoder;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Log4jConfigurator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoCodeCsv {
	private static final GeoCoder[] CODERS = new GeoCoder[]{
		new NaverGeoCoder(),
		new GoogleGeoCoder(),
	};
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		Log4jConfigurator.setLevelAll(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_geocode ");
		parser.addArgOption("input", "path", "input csv-file path", true);
		parser.addArgOption("addr_idx", "num", "input address column indexes", true);
		parser.addArgOption("output", "name", "output layer name", true);
		parser.addArgOption("charset", "name", "character encoding (default: euc-kr)", false);
		parser.addOption("help", "show usage", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		File input = cl.getFile("input");
		String output = cl.getString("output");
		int[] addrIdxs = Arrays.stream(cl.getString("addr_idx").split(":"))
								.mapToInt(Integer::parseInt)
								.toArray();
		Charset charset = Charset.forName(cl.getOptionString("charset").getOrElse("utf-8"));
		
		CSVFormat format = CSVFormat.DEFAULT
									.withQuote('"')
									.withDelimiter(',')
									.withIgnoreEmptyLines();
		
		PrintWriter writer = new PrintWriter(new File(output), charset.name());
		PrintWriter writer2 = new PrintWriter(new File(output + ".err"), charset.name());
		
		CSVPrinter printer = new CSVPrinter(writer, format);
		CSVPrinter printer2 = new CSVPrinter(writer2, format);
		
		int count = 0;
		
		CSVParser reader = CSVParser.parse(input, charset, format);
		for ( CSVRecord rec: reader ) {
			List<String> cols = Lists.newArrayList();
			for ( String col: rec ) {
				cols.add(col);
			}
			
			boolean done = false;
			for ( int i =0; i < addrIdxs.length && !done; ++i ) {
				String addr = rec.get(addrIdxs[i]).trim();
				if ( addr.length() > 0 ) {
					addr = addr.replaceAll("\\(.*\\)", "");
					System.out.printf("%4d: addr=%s", ++count, addr);
					
					Coordinate loc = getCoordinate(addr);
					if ( loc != null ) {
						cols.add("" + loc.y);
						cols.add("" + loc.x);
						printer.printRecord(cols);
						done = true;
						
						System.out.printf(" -> %.3f,%.3f%n", loc.y, loc.x);
					}
					else {
						System.out.printf(" -> failed%n");
					}
				}
			}
			if ( !done ) {
				printer2.printRecord(cols);
			}
			
			printer.flush();
			printer2.flush();
		}
		printer.close();
		printer2.close();
	}
	
	private static Coordinate getCoordinate(String addr) {
		addr = addr.replaceAll("\\(.*\\)", "");
		for ( int i =0; i < CODERS.length; ++i ) {
			try {
				List<Coordinate> loc = CODERS[i].getWgs84Location(addr);
				if ( loc.size() > 0 ) {
					return loc.get(0);
				}
			}
			catch ( Throwable ignored ) { }
		}
		
		return null;
	}
}
