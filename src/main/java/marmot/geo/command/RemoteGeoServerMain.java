package marmot.geo.command;

import marmot.MarmotRuntime;
import marmot.command.MarmotClientCommand;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.command.RemoteGeoServerMain.Add;
import marmot.geo.command.RemoteGeoServerMain.Delete;
import marmot.geo.command.RemoteGeoServerMain.ListDataSet;
import marmot.geo.geoserver.rest.GeoServer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mc_geoserver",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="GeoServer-related commands",
		subcommands = {
			ListDataSet.class, Add.class, Delete.class,
		})
public class RemoteGeoServerMain extends MarmotClientCommand {
	@Option(names={"-ghost"}, paramLabel="ip",
			description={"IP address of GeoServer (default: localhost)"})
	private String m_host = "localhost";
	
	@Option(names={"-gport"}, paramLabel="port_number",
			description={"port number of GeoServer (default: 8080)"})
	private int m_port = 8080;

	public static final void main(String... args) throws Exception {
		MarmotClientCommands.configureLog4j();

		RemoteGeoServerMain cmd = new RemoteGeoServerMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	static abstract class AbstractGeoServerCommand extends PicocliSubCommand<MarmotRuntime> {
		abstract protected void run(MarmotRuntime marmot, GeoServer server) throws Exception;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			RemoteGeoServerMain parent = (RemoteGeoServerMain)getParent();
			GeoServer geoServer = GeoServer.create(parent.m_host, parent.m_port, "admin", "geoserver");
			
			run(initialContext, geoServer);
		}
	}
	
	@Command(name="list", description="list published datasets through GeoServer")
	public static class ListDataSet extends AbstractGeoServerCommand {
		@Override
		public void run(MarmotRuntime marmot, GeoServer server) throws Exception {
			for ( String id: server.listLayers() ) {
				System.out.println(id);
			}
		}
	}
	
	@Command(name="add", description="publish dataset through GeoServer")
	public static class Add extends AbstractGeoServerCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot, GeoServer server) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			server.addLayer(ds);
		}
	}
	
	@Command(name="delete", aliases= {"remove"}, description="unpublish dataset from GeoServer")
	public static class Delete extends AbstractGeoServerCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot, GeoServer server) throws Exception {
			server.removeLayer(m_dsId);
		}
	}
}
