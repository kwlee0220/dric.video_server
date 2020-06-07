package dric.camera;

import java.io.File;
import java.util.Map;

import org.opencv.videoio.VideoWriter;

import dric.ConfigUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class VideoConfig {
	private final File m_videoDir;
	private final float m_fps;
	private final char[] m_fourcc;
	private final long m_tailInterval;
	
	private VideoConfig(File videoFolder, float fps, char[] fourcc, long tailInterval) {
		m_videoDir = videoFolder;
		m_fps = fps;
		m_fourcc = fourcc;
		m_tailInterval = tailInterval;
	}
	
	static VideoConfig from(Map<String,Object> config) {
		File videoDir = new File(ConfigUtils.parseString(config, "folder"));
		float fps = ConfigUtils.parseFloat(config, "fps"); 
		char[] fourcc = ConfigUtils.parseString(config, "fourcc")
									.trim().toUpperCase()
									.substring(0, 4)
									.toCharArray();
		long interval = ConfigUtils.parseDuration(config, "tail_interval");
				
		return new VideoConfig(videoDir, fps, fourcc, interval);
	}
	
	public File getVideoDir() {
		return m_videoDir;
	}
	
	public String getFourccString() {
		return new String(m_fourcc);
	}
	
	public int getFourcc() {
		return VideoWriter.fourcc(m_fourcc[0], m_fourcc[1], m_fourcc[2], m_fourcc[3]);
	}
	
	public float getFps() {
		return m_fps;
	}
	
	public long getTailInterval() {
		return m_tailInterval;
	}
}