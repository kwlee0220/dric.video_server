package dric.video;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import dric.ConfigUtils;
import dric.proto.EndPoint;
import dric.proto.JdbcEndPoint;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VideoServerConfig {
	private final EndPoint m_vserverEp;
	private final EndPoint m_platformEp;
	private final File m_videoTailDir;
	private final JdbcEndPoint m_jdbcEp;
	private final List<File> m_openCvDllList;
	
	private VideoServerConfig(EndPoint vserverEp, EndPoint platformEp, File videoTailDir,
								JdbcEndPoint jdbcEp, List<File> openCvDllFiles) {
		m_vserverEp = vserverEp;
		m_platformEp = platformEp;
		m_videoTailDir = videoTailDir;
		m_jdbcEp = jdbcEp;
		m_openCvDllList = openCvDllFiles;
	}
	
	public static VideoServerConfig from(File configFile, Map<String,String> bindings)
		throws FileNotFoundException, IOException {
		return from(ConfigUtils.readYaml(configFile, bindings));
	}
	
	public static VideoServerConfig from(Map<String,Object> props) {
		EndPoint videoServerEp = ConfigUtils.parseEndPoint(props, "video_server");
		EndPoint platformEp = ConfigUtils.parseEndPoint(props, "dric_platform");
		Map<String,Object> videoConf = ConfigUtils.getSubConfig(props, "video");
		File tailFolder = new File(ConfigUtils.parseString(videoConf, "tail_folder"));
		JdbcEndPoint jdbc = ConfigUtils.parseJdbcEndPoint(props, "jdbc");
		List<File> dllFileList = ConfigUtils.parseOpenCvDllFiles(props, "opencv_dlls");

		return new VideoServerConfig(videoServerEp, platformEp, tailFolder, jdbc, dllFileList);
	}
	
	public EndPoint getVideoServerEndPoint() {
		return m_vserverEp;
	}
	
	public EndPoint getPlatformEndPoint() {
		return m_platformEp;
	}
	
	public File getVideoTailFolder() {
		return m_videoTailDir;
	}
	
	public JdbcEndPoint getJdbcEndPoint() {
		return m_jdbcEp;
	}
	
	public List<File> getOpenCvDllList() {
		return m_openCvDllList;
	}
}
