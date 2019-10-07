package marmot.command;

import java.io.IOException;

import marmot.MarmotRuntime;
import marmot.command.PicocliCommands.PicocliCommand;
import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotClientCommand implements PicocliCommand {
	@Spec private CommandSpec m_spec;
	@Mixin private MarmotConnector m_connector;
	@Mixin private UsageHelp m_help;
	private PBMarmotClient m_marmot;
	
	protected void run(MarmotRuntime marmot) throws Exception {
		m_spec.commandLine().usage(System.out, Ansi.OFF);
	}
	
	@Override
	public PBMarmotClient getMarmotRuntime() {
		try {
			if ( m_marmot == null ) {
				m_marmot = m_connector.connect();
			}
			
			return m_marmot;
		}
		catch ( IOException e ) {
			Throwables.sneakyThrow(e);
			throw new AssertionError();
		}
	}
	
	@Override
	public void run() {
		try {
			run(getMarmotRuntime());
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
}