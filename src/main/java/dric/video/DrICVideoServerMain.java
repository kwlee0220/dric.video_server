package dric.video;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.ConfigUtils;
import dric.proto.EndPoint;
import dric.video.grpc.PBDrICVideoServerServant;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import opencvj.OpenCvInitializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import utils.NetUtils;
import utils.UsageHelp;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="dric_video_server",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="DrIC VideoServer command")
public class DrICVideoServerMain implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICVideoServerMain.class);
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Option(names={"--config"}, paramLabel="path", description={"VideoServer configration file"})
	private File m_configFile = new File("video_server.yaml");
	
	@Option(names={"-f", "--format"}, description={"format DrICVideoServer database"})
	private boolean m_format = false;
	
	public static final void main(String... args) throws Exception {
		configureLog4j();

		DrICVideoServerMain cmd = new DrICVideoServerMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	@Override
	public void run() {
		try {
			configureLog4j();
			
			VideoServerConfig config = VideoServerConfig.from(m_configFile);
			DrICVideoServerImpl videoServer = new DrICVideoServerImpl(config);
			
			if ( m_format ) {
				JdbcProcessor jdbc = ConfigUtils.getJdbcProcessor(config.getJdbcEndPoint());
				try ( Connection conn = jdbc.connect() ) {
					DrICVideoServerImpl.format(conn);
				}
			}
			
			loadOpenCv(config);
			
			EndPoint ep = config.getVideoServerEndPoint();
			Server server = createServer(videoServer, ep.getPort());
	    	Runtime.getRuntime().addShutdownHook(new Thread() {
	    		public void run() {
	    			server.shutdown();
	    		}
	    	});
			server.start();

			String host = NetUtils.getLocalHostAddress();
			System.out.printf("started: DrICVideoServer[host=%s, port=%d]%n", host, ep.getPort());
			server.awaitTermination();
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
//			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("DRIC_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		PropertyConfigurator.configure(props);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
		
		return propsFile;
	}
	
	private Server createServer(DrICVideoServerImpl server, int port) {
		PBDrICVideoServerServant servant = new PBDrICVideoServerServant(server);
		Server nettyServer = NettyServerBuilder.forPort(port)
												.addService(servant)
												.build();
		return nettyServer;
	}

	private void loadOpenCv(VideoServerConfig config) throws IOException {
		if ( config.getOpenCvDllList().size() > 0 ) {
			OpenCvInitializer.initialize(config.getOpenCvDllList());
		}
		else {
			OpenCvInitializer.initialize();
		}
	}
}
