package marmot.externio.excel;

import java.util.List;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import picocli.CommandLine.Option;
import utils.CSV;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExcelParameters {
	private boolean m_headerFirst = false;
	private FOption<String> m_nullString = FOption.empty();
	private FOption<String> m_pointCols = FOption.empty();
	private FOption<String> m_excelSrid = FOption.empty();
	
	public static ExcelParameters create() {
		return new ExcelParameters();
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"-header_first"}, description="consider the first line as header")
	public ExcelParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}
	
	public FOption<String> nullString() {
		return m_nullString;
	}

	@Option(names={"-null_string"}, paramLabel="null-string",
			description="null string for column value")
	public ExcelParameters nullString(String nullString) {
		m_nullString = FOption.ofNullable(nullString);
		return this;
	}

	@Option(names={"-point_col"}, paramLabel="point_columns", description="X,Y fields for point")
	public ExcelParameters pointColumn(String pointCols) {
		Utilities.checkNotNullArgument(pointCols, "Point columns are null");
		
		m_pointCols = FOption.ofNullable(pointCols);
		return this;
	}
	
	public FOption<Tuple2<String,String>> pointColumn() {
		return m_pointCols.map(cols -> {
			List<String> parts = CSV.parseCsv(cols, ',', '\\').toList();
			return Tuple.of(parts.get(0), parts.get(1));
		});
	}

	@Option(names={"-excel_srid"}, paramLabel="srid", description="EPSG code for input Excel file")
	public ExcelParameters excelSrid(String srid) {
		m_excelSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> excelSrid() {
		return m_excelSrid;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst ? ", header" : "";
		String ptStr = pointColumn().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_excelSrid.map(s -> String.format(", csv_srid=%s", s))
									.getOrElse("");
		return String.format("%s%s%s", headerFirst, ptStr, srcSrid);
	}
}