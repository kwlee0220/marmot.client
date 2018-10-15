package marmot.geo.command;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.geo.GeoClientUtils;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.Size2i;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGridAnalysis {
	private static final String INPUT = "tmp/anyang/cadastral_electro";
	private static final String OUTPUT = "tmp/anyang/land_electro";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_export_as_shapefile ");
		parser.addArgumentName("in_dataset");
		parser.addArgumentName("out_dataset");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("cell_size", "size", "grid-cell size", false);
		parser.addArgOption("grid_dim", "size", "grid-cell dimension", false);
		parser.addArgOption("values", "csv", "value column name list", true);
		parser.addArgOption("tiff", "path", "target tiff file path (or prefix)", false);
		parser.addOption("f", "delete the output dataset if it exists already", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		try {
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			String input = cl.getArgument("in_dataset");
			String output = cl.getArgument("out_dataset");
			String cellSizeStr = cl.getOptionString("cell_size").getOrNull();  
			String gridDimStr = cl.getOptionString("grid_dim").getOrNull();
			List<String> valueColNames = CSV.parse(cl.getString("values"), ',', '\\');
			String tiffPath = cl.getOptionString("tiff").getOrNull();
			boolean force = cl.hasOption("f");
			
			if ( cellSizeStr == null && gridDimStr == null ) {
				System.err.println("grid cell size is not specified: "
								+ "use '-cell_size' or '-grid_dim'");
				System.exit(-1);
			}
			
			StopWatch watch = StopWatch.start();
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			DataSet ds = marmot.getDataSet(input);
			Envelope bounds = ds.getBounds();
			
			Size2i gridDim;
			Size2d cellSize;
			if ( cellSizeStr != null ) {
				cellSize = Size2d.fromString(cellSizeStr);
				gridDim = GeoClientUtils.divide(bounds, cellSize).ceilToInt();
			}
			else {
				gridDim = Size2i.fromString(gridDimStr);
				cellSize = GeoClientUtils.divide(bounds, gridDim);
			}
			
			String planName = "격자 분석";
			String updateExpr = valueColNames.stream()
											.map(col -> String.format("%s *= __ratio", col))
											.collect(Collectors.joining("; "));
			List<AggregateFunction> aggrs = valueColNames.stream()
														.map(col -> AggregateFunction.SUM(col).as(col))
														.collect(Collectors.toList());
			
			Plan plan;
			plan = marmot.planBuilder(planName)
						.load(input)
						.assignSquareGridCell("the_geom", new SquareGrid(bounds, cellSize))
						.intersection("the_geom", "cell_geom", "__overlap")
						.expand("__ratio:double",
								"__ratio = (ST_Area(__overlap) /  ST_Area(the_geom))")
						.update(updateExpr)
						.groupBy("cell_id")
							.tagWith("cell_geom,cell_pos")
							.aggregate(aggrs)
						.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
						.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y,cell_id,cell_pos}")
						.store(output)
						.build();
			GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
															ds.getGeometryColumnInfo().srid());
			DataSet result = marmot.createDataSet(output, plan, DataSetOption.GEOMETRY(gcInfo),
													DataSetOption.FORCE);
			System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
//			DataSet result = marmot.getDataSet("tmp/anyang/grid_gas");
			
//			if ( tiffPath != null ) {
//				if ( valueColNames.size() == 1 ) {
//					String colName = valueColNames.get(0);
//					writeAsTiff(marmot, result, gridDim, colName, new File(tiffPath));
//				}
//			}
			
			marmot.disconnect();
		}
		catch ( Exception e ) {
			e.printStackTrace(System.err);
			cl.exitWithUsage(0);
		}
	}
	
/*
	private static void writeAsTiff(PBMarmotClient marmot, DataSet ds, Size2i gridDim,
									String colName, File tiffFile) throws IOException {
		Rasters rasters = new Rasters(gridDim.getWidth(), gridDim.getHeight(), 1, FieldType.FLOAT);

		int rowsPerStrip = rasters.calculateRowsPerStrip(TiffConstants.PLANAR_CONFIGURATION_CHUNKY);

		FileDirectory directory = new FileDirectory();
		directory.setImageWidth(gridDim.getWidth());
		directory.setImageHeight(gridDim.getHeight());
		directory.setBitsPerSample(32);
		directory.setCompression(TiffConstants.COMPRESSION_NO);
		directory.setPhotometricInterpretation(TiffConstants.PHOTOMETRIC_INTERPRETATION_BLACK_IS_ZERO);
		directory.setSamplesPerPixel(1);
		directory.setRowsPerStrip(rowsPerStrip);
		directory.setPlanarConfiguration(TiffConstants.PLANAR_CONFIGURATION_CHUNKY);
		directory.setSampleFormat(TiffConstants.SAMPLE_FORMAT_FLOAT);
		directory.setWriteRasters(rasters);
		
		try ( RecordSet rset = ds.read() ) {
			Record record = DefaultRecord.of(ds.getRecordSchema());
			
			while ( rset.next(record) ) {
				int x = record.getInt("x", -1);
				int y = record.getInt("y", -1);
				float v = record.getFloat(colName, -1);
				
				rasters.setFirstPixelSample(x, y, v);
			}
		}

		TIFFImage tiffImage = new TIFFImage();
		tiffImage.add(directory);
		TiffWriter.writeTiff(tiffFile, tiffImage);
	}
*/
}
