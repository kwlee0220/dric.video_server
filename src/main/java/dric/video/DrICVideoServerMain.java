package dric.video;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import dric.proto.EndPoint;
import dric.video.grpc.PBDrICVideoServerServant;
import dric.video.sunapi.SunApiVideoServerImpl;
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
import utils.Utilities;

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
	private static final String ENV_VAR_HOME = "DRIC_VIDEO_HOME";
	private static final String DEFAULT_CONFIG_FNAME = "video_server.yaml";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	private File m_homeDir;
	
	@Option(names={"--config"}, paramLabel="path", description={"VideoServer configration file"})
	private File m_configFile;
	
	@Option(names={"-f", "--format"}, description={"format DrICVideoServer database"})
	private boolean m_format = false;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	public static final void main(String... args) throws Exception {
		DrICVideoServerMain cmd = new DrICVideoServerMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	@Option(names={"--home"}, paramLabel="path", description={"DrICVideoServer Home Directory"})
	void setHomeDir(File dir) {
		try {
			m_homeDir = dir.getCanonicalFile();
		}
		catch ( IOException e ) {
			throw new IllegalArgumentException("invalid home.dir=" + dir, e);
		}
	}
	
	@Override
	public void run() {
		try {
			if ( m_verbose ) {
				System.out.println("use home.dir: " + getHomeDir());
			}
			configureLog4j();

			File configFile = getConfigFile();
			if ( m_verbose ) {
				System.out.println("use configuration file: " + configFile);
			}
			
			Map<String,String> bindings = Maps.newHashMap();
			bindings.put("dric.video.home", getHomeDir().getAbsolutePath());
			VideoServerConfig config = VideoServerConfig.from(configFile, bindings);
//			DrICVideoServerImpl videoServer = new DrICVideoServerImpl(config);
			SunApiVideoServerImpl videoServer = new SunApiVideoServerImpl("129.254.82.33", 80, "admin", "dr.icTop!!");
			
			if ( m_format ) {
//				JdbcProcessor jdbc = ConfigUtils.getJdbcProcessor(config.getJdbcEndPoint());
//				try ( Connection conn = jdbc.connect() ) {
//					if ( m_verbose ) {
//						System.out.println("format database");
//					}
//					
//					DrICVideoServerImpl.format(conn);
//				}
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
			if ( m_verbose ) {
				System.out.printf("started: DrICVideoServer[host=%s, port=%d]%n", host, ep.getPort());
			}
			server.awaitTermination();
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
//			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	private File getConfigFile() {
		if ( m_configFile == null ) {
			return new File(getHomeDir(), DEFAULT_CONFIG_FNAME);
		}
		else {
			return m_configFile;
		}
	}
	
	private Server createServer(DrICVideoServer server, int port) {
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
	
	public File getHomeDir() {
		File homeDir = m_homeDir;
		if ( homeDir == null ) {
			String homeDirPath = System.getenv(ENV_VAR_HOME);
			if ( homeDirPath == null ) {
				return Utilities.getCurrentWorkingDir();
			}
			else {
				return new File(homeDirPath);
			}
		}
		else {
			return m_homeDir;
		}
	}

	public void configureLog4j() throws IOException {
		File propsFile = new File(getHomeDir(), "log4j.properties");
		if ( m_verbose ) {
			System.out.printf("use log4j.properties: file=%s%n", propsFile);
		}
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		PropertyConfigurator.configure(props);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
}
