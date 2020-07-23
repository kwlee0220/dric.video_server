package dric.camera;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.DrICClient;
import dric.proto.CameraInfo;
import dric.topic.Topic;
import dric.topic.TopicClient;
import dric.type.CameraFrame;
import dric.video.PBDrICVideoServerProxy;
import opencvj.OpenCvInitializer;
import picocli.CommandLine;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import utils.UsageHelp;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICCameraAgentMain implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICCameraAgentMain.class);
	private static final String ENV_VAR_HOME = "DRIC_CAMERA_HOME";
	private static final String DEF_CONFIG_FILE = "camera_agent.yaml";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="camera-id", index="0", description={"camera id"})
	private String m_cameraId;
	
	private File m_homeDir;
	@Option(names={"--home"}, paramLabel="path", description={"DrICVideoServer Home Directory"})
	public void setHome(String path) throws IOException {
		m_homeDir = new File(path).getCanonicalFile();
	}
	
	@Option(names={"--config"}, paramLabel="path", description={"CameraAgent configuration file"})
	private File m_configFile;
	
	@Option(names={"-n", "--no-video"}, description={"do not create video files"})
	private boolean m_noVideo = false;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	private TopicClient m_client = null;
	
	public static final void main(String... args) throws Exception {
		DrICCameraAgentMain cmd = new DrICCameraAgentMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	@Override
	public void run() {
		try  {
			configureLog4j();

			File configFile = getConfigFile();
			if ( m_verbose ) {
				System.out.println("use configuration file: " + configFile);
			}
			CameraAgentConfig config = CameraAgentConfig.from(configFile);
			if ( config.getOpenCvDllList().size() > 0 ) {
				OpenCvInitializer.initialize(config.getOpenCvDllList());
			}
			else {
				OpenCvInitializer.initialize();
			}
			
			DrICClient client = DrICClient.connect(config.getPlatformEndPoint());
	    	
	    	CameraInfo camInfo = null;
			try ( PBDrICVideoServerProxy vserver = client.getVideoServer() ) {
				camInfo = vserver.getCamera(m_cameraId);
			}
			if ( m_verbose ) {
				float fps = config.getVideoConfig().getFps();
				System.out.printf("camera=%s, url=%s, fps=%.1f, fourcc=%s, dir=%s%n", camInfo.getId(), camInfo.getRtspUrl(),
									fps, config.getVideoConfig().getFourccString(), config.getVideoConfig().getVideoDir());
			}
			
			m_client = client.getTopicClient(camInfo.getId());
			Topic<CameraFrame> topic = m_client.getCameraFrameTopic();
	    	Runtime.getRuntime().addShutdownHook(new Thread() {
	    		public void run() {
	    			if ( m_client != null ) {
	    				if ( s_logger.isInfoEnabled() ) {
	    					s_logger.info("disconnect from MQTT-Server");
	    				}
	    				
	    				if ( m_client != null ) {
	    					m_client.disconnect();
		    				m_client = null;
	    				}
	    			}
	    		}
	    	});
	    	
			DrICCameraAgent agent = new DrICCameraAgent(camInfo, topic, m_noVideo, config);
			agent.run();
		}
		catch ( Throwable e ) {
			System.err.printf("failed: %s%n%n", e);
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
		finally {
			if ( m_client != null ) {
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("disconnect from MQTT-Server");
				}
				
				if ( m_client != null ) {
					m_client.disconnect();
    				m_client = null;
				}
			}
		}
	}
	
	private File getHomeDir() {
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
	
	private File getConfigFile() {
		if ( m_configFile == null ) {
			return new File(getHomeDir(), DEF_CONFIG_FILE);
		}
		else {
			return m_configFile;
		}
	}

	private void configureLog4j() throws IOException {
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
