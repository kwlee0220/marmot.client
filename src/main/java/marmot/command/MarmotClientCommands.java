package marmot.command;

import java.io.File;
import java.io.IOException;

import utils.CommandLine;
import utils.func.FOption;

import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotClientCommands {
	private static final int DEFAULT_MARMOT_PORT = 12985;
	
	private MarmotClientCommands() {
		throw new AssertionError("Should not be called: class=" + MarmotClientCommands.class);
	}
	
	public static PBMarmotClient connect() throws IOException {
		String host = MarmotClientCommands.getMarmotHost();
		int port = MarmotClientCommands.getMarmotPort();
		
		return PBMarmotClient.connect(host, port);
	}
	
	public static String getMarmotHost() {
		String host = System.getenv("MARMOT_HOST");
		if ( host != null ) {
			return host;
		}
		
		return "localhost";
	}
	
	public static int getMarmotPort() {
		String portStr = System.getenv("MARMOT_PORT");
		if ( portStr != null ) {
			return Integer.parseInt(portStr);
		}

		return DEFAULT_MARMOT_PORT;
	}
	
	public static PBMarmotClient connect(CommandLine cl) throws IOException {
		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		return PBMarmotClient.connect(host, port);
	}
	
	public static String getMarmotHost(CommandLine cl) {
		return getMarmotHost(cl, "host");
	}
	
	public static String getMarmotHost(CommandLine cl, String optId) {
		String host = cl.getOptionString(optId).getOrNull();
		if ( host != null ) {
			return host;
		}
		
		return getMarmotHost();
	}
	
	public static int getMarmotPort(CommandLine cl) {
		return getMarmotPort(cl, "port");
	}
	
	public static int getMarmotPort(CommandLine cl, String optId) {
		FOption<Integer> port = cl.getOptionInt(optId);
		if ( port.isPresent() ) {
			return port.get();
		}
		
		return getMarmotPort();
	}
	
	public static void configureLog4j() {
		String configFileName = FOption.ofNullable(System.getenv("MARMOT_CLIENT_HOME"))
									.map(File::new)
									.map(parent -> new File(parent, "log4j.properties"))
									.map(file -> file.toString())
									.getOrElse("log4j.properties");
	}
	
	public static void disableLog4j() {
	}
}
