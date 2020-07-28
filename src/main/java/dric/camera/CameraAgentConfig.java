package dric.camera;

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
public class CameraAgentConfig {
	private final EndPoint m_platformEp;
	private final VideoConfig m_videoConfig;
	private final JdbcEndPoint m_jdbcEp;
	private final List<File> m_openCvDllList;
	
	private CameraAgentConfig(EndPoint ep, VideoConfig videoConfig, JdbcEndPoint jdbcEp,
								List<File> openCvDllFiles) {
		m_platformEp = ep;
		m_videoConfig = videoConfig;
		m_jdbcEp = jdbcEp;
		m_openCvDllList = openCvDllFiles;
	}
	
	public static CameraAgentConfig from(File configFile, Map<String,String> bindings)
		throws FileNotFoundException, IOException {
		return from(ConfigUtils.readYaml(configFile, bindings));
	}
	
	@SuppressWarnings("unchecked")
	public static CameraAgentConfig from(Map<String,Object> config) {
		EndPoint platformEp = ConfigUtils.parseEndPoint(config, "dric_platform");
		VideoConfig videoConfig = VideoConfig.from((Map<String,Object>)config.get("video_store"));
		JdbcEndPoint jdbc = ConfigUtils.parseJdbcEndPoint(config, "jdbc");
		List<File> dllFileList = ConfigUtils.parseOpenCvDllFiles(config, "opencv_dlls");

		return new CameraAgentConfig(platformEp, videoConfig, jdbc, dllFileList);
	}
	
	public EndPoint getPlatformEndPoint() {
		return m_platformEp;
	}
	
	public VideoConfig getVideoConfig() {
		return m_videoConfig;
	}
	
	public JdbcEndPoint getJdbcEndPoint() {
		return m_jdbcEp;
	}
	
	public List<File> getOpenCvDllList() {
		return m_openCvDllList;
	}
}
