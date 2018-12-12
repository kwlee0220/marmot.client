package marmot.command;

import java.io.File;

import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadRemoteFilesMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_upload_files ");
		parser.addArgumentName("src");
		parser.addArgumentName("dest_dir");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		parser.addArgOption("block_size", "nbytes", "block size in bytes", false);
		parser.addArgOption("glob", "glob", "path matcher", false);
		parser.addOption("f", "force to create a new dataset", false);
		parser.addOption("h", "show usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
	
			String host = MarmotClientCommands.getMarmotHost(cl);
			int port = MarmotClientCommands.getMarmotPort(cl);
			
			File start = new File(cl.getArgument(0));
			String dest = cl.getArgument(1);
			FOption<Long> blockSize = cl.getOptionString("block_size")
										.map(UnitUtils::parseByteSize);
			String glob = cl.getOptionString("glob").getOrNull();
			boolean force = cl.hasOption("f");
			
			// 원격 MarmotServer에 접속.
			PBMarmotClient marmot = PBMarmotClient.connect(host, port);
			
			UploadFiles upload = new UploadFiles(marmot, start, dest)
										.glob(glob)
										.blockSize(blockSize)
										.force(force);
			upload.run();
			
			marmot.disconnect();
		}
		catch ( Exception e ) {
			System.err.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
