package marmot.command;

import marmot.MarmotRuntime;
import marmot.command.DatasetCommands.ImportCsvCmd;
import marmot.command.DatasetCommands.ImportGeoJsonCmd;
import marmot.command.DatasetCommands.ImportJdbcCmd;
import marmot.command.DatasetCommands.ImportShapefileCmd;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.RemoteDataSetMain.RemoteImport;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			DatasetCommands.ListDataSet.class,
			DatasetCommands.Show.class,
			DatasetCommands.Schema.class,
			DatasetCommands.Move.class,
			DatasetCommands.SetGcInfo.class,
			DatasetCommands.AttachGeometry.class,
			DatasetCommands.Count.class,
			DatasetCommands.Bind.class,
			DatasetCommands.Delete.class,
			RemoteImport.class,
			DatasetCommands.Export.class,
			DatasetCommands.Thumbnail.class,
		})
public class RemoteDataSetMain extends MarmotClientCommand {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteDataSetMain cmd = new RemoteDataSetMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}

	@Command(name="import",
			subcommands= {
				ImportCsvCmd.class,
				ImportShapefileCmd.class,
				ImportGeoJsonCmd.class,
				ImportExcelCmd.class,
				ImportJdbcCmd.class,
			},
			description="import into the dataset")
	public static class RemoteImport extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception { }
	}
}
