package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

import marmot.plan.STScriptPlanLoader;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintStPlanMain {
	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		CommandLineParser parser = new CommandLineParser("mc_print_plan ");
		parser.addArgumentName("plan_file");
		parser.addOption("h", "help usage", false);
		
		try {
			CommandLine cl = parser.parseArgs(args);
			if ( cl.hasOption("h") ) {
				cl.exitWithUsage(0);
			}
			
			String script;
			if ( cl.getArgumentCount() > 0 ) {
				File file = new File(cl.getArgument("plan_file"));
				script = IOUtils.toString(file);
			}
			else {
				try ( InputStream is = System.in ) {
					script = IOUtils.toString(is, Charset.defaultCharset());
				}
			}
			String planJson = STScriptPlanLoader.toJson(script);
			System.out.println(planJson);
		}
		catch ( Exception e ) {
			System.out.println("" + e);
			parser.exitWithUsage(-1);
		}
	}
}
