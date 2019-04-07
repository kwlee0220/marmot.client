package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

import marmot.plan.STScriptPlanLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_print_plan", description="display JSON plan description",
		mixinStandardHelpOptions=false)
public class PrintStPlanMain implements Runnable {
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="plan_file", arity = "0..*",
				description={"path to the target template plan file"})
	private String m_planFile = null;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();
		
		PrintStPlanMain cmd = new PrintStPlanMain();
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
			String script;
			if ( m_planFile != null ) {
				script = IOUtils.toString(new File(m_planFile));
			}
			else {
				try ( InputStream is = System.in ) {
					script = IOUtils.toString(is, Charset.defaultCharset());
				}
			}
			String planJson = STScriptPlanLoader.parseToPlanJson(script);
			System.out.println(planJson);
		}
		catch ( Exception e ) {
			System.err.println(e);
		}
	}
}
