package dric.camera;

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

import dric.DrICClient;
import dric.topic.TopicClient;
import dric.type.CameraFrame;
import marmot.dataset.DataSet;
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
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICVideoPlayerMain implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICVideoPlayerMain.class);
	private static final String ENV_VAR_HOME = "DRIC_CAMERA_HOME";
	private static final String DEF_CONFIG_FILE = "camera_agent.yaml";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="camera-id", index="0", description={"camera id"})
	private String m_cameraId;
	
	@Parameters(paramLabel="video-file", index="1", description={"video file path"})
	private String m_videoFile;

	private File m_homeDir;
	
	@Option(names={"--config"}, paramLabel="path", description={"DrICVideoPlayer configuration file"})
	private File m_configFile;
	
	@Option(names={"--fps"}, paramLabel="fps", description={"frames per second"})
	private float m_fps = 0f;
	
	@Option(names={"-l", "--loop"}, description={"loop"})
	private boolean m_loop = false;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	private TopicClient m_client = null;
	
	public static final void main(String... args) throws Exception {
		DrICVideoPlayerMain cmd = new DrICVideoPlayerMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}

	@Option(names={"--home"}, paramLabel="path", description={"DrICVideoPlayer Home Directory"})
	public void setHomeDir(File file) {
		try {
			m_homeDir = file.getCanonicalFile();
		}
		catch ( IOException e ) {
			throw new IllegalArgumentException("invalid home.dir=" + file);
		}
	}
	
	@Override
	public void run() {
		try  {
			if ( m_verbose ) {
				System.out.println("use home.dir: " + getHomeDir());
			}
			configureLog4j();

			File configFile = getConfigFile();
			if ( m_verbose ) {
				System.out.println("use configuration file: " + configFile);
			}
			
			Map<String,String> bindings = Maps.newHashMap();
			bindings.put("dric.camera.home", getHomeDir().getAbsolutePath());
			CameraAgentConfig config = CameraAgentConfig.from(configFile, bindings);
			if ( config.getOpenCvDllList().size() > 0 ) {
				OpenCvInitializer.initialize(config.getOpenCvDllList());
			}
			else {
				OpenCvInitializer.initialize();
			}
			
			DrICClient client = DrICClient.connect(config.getPlatformEndPoint(), m_cameraId);
			DataSet topic = client.getDataSet(CameraFrame.DATASET_ID);
			DrICVideoPlayer agent = new DrICVideoPlayer(m_cameraId, m_videoFile, m_fps, topic);
			do {
				agent.run();
			} while ( m_loop );
		}
		catch ( Throwable e ) {
			System.err.printf("failed: %s%n%n", e);
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
		finally {
			IOUtils.closeQuietly(m_client);
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
	
	private File getConfigFile() {
		if ( m_configFile == null ) {
			return new File(getHomeDir(), DEF_CONFIG_FILE);
		}
		else {
			return m_configFile;
		}
	}
}
