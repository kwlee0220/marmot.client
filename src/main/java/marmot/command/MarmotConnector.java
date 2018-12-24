package marmot.command;

import java.io.IOException;

import marmot.remote.protobuf.PBMarmotClient;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotConnector {
	private static final int DEFAULT_MARMOT_PORT = 12985;
	
	private String m_host;
	private int m_port = -1;
	
	public PBMarmotClient connect() throws IOException {
		return PBMarmotClient.connect(getMarmotHost(), getMarmotPort());
	}
	
	public String getMarmotHost() {
		return m_host != null ? m_host : getDefaultMarmotHost();
	}

	@Option(names={"-host"}, paramLabel="ip_addr", description={"marmot server host (default: localhost)"})
	public void setMarmotHost(String host) {
		m_host = host;
	}
	
	public int getMarmotPort() {
		return m_port > 0 ? m_port : getDefaultMarmotPort();
	}

	@Option(names={"-port"}, paramLabel="port_no", description={"marmot server port (default: 12985)"})
	public void setMarmotPort(int port) {
		m_port = port;
	}
	
	public static String getDefaultMarmotHost() {
		String host = System.getenv("MARMOT_HOST");
		if ( host != null ) {
			return host;
		}
		
		return "localhost";
	}
	
	public static int getDefaultMarmotPort() {
		String portStr = System.getenv("MARMOT_PORT");
		if ( portStr != null ) {
			return Integer.parseInt(portStr);
		}

		return DEFAULT_MARMOT_PORT;
	}
}
