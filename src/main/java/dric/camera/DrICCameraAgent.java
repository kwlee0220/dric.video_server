package dric.camera;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.opencv.core.Size;
import org.opencv.videoio.VideoCapture;
import org.opencv.videoio.VideoWriter;
import org.opencv.videoio.Videoio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.ConfigUtils;
import dric.proto.CameraInfo;
import marmot.dataset.DataSet;
import utils.LocalDateTimes;
import utils.func.CheckedRunnable;
import utils.func.Tuple;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICCameraAgent implements CheckedRunnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICCameraAgent.class);
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS");
	
	private final CameraInfo m_info;
	private final DataSet m_topic;
	private final CameraAgentConfig m_config;
	private final boolean m_noVideo;
	private final JdbcProcessor m_jdbc;
	
	private volatile Size m_resol;
	private volatile VideoCapture m_camera;
	
	public DrICCameraAgent(CameraInfo info, DataSet topic, boolean noVideo, CameraAgentConfig config) {
		m_info = info;
		m_topic = topic;
		m_noVideo = noVideo;
		m_config = config;
		m_jdbc = ConfigUtils.getJdbcProcessor(config.getJdbcEndPoint());
	}
	
	String getCameraId() {
		return m_info.getId();
	}
	
	Size getResolution() {
		return m_resol;
	}
	
	JdbcProcessor getJdbcProcessor() {
		return m_jdbc;
	}
	
	float getFps() {
		return m_config.getVideoConfig().getFps();
	}
	
	long getVideoTailInterval() {
		return m_config.getVideoConfig().getTailInterval();
	}
	
	DataSet getCameraFrameTopic() {
		return m_topic;
	}
	
	@Override
	public void run() throws Throwable {
		m_camera = new VideoCapture();
		if ( !m_camera.open(m_info.getRtspUrl()) ) {
			throw new IllegalArgumentException("fails to open camera: rtps_url=" + m_info.getRtspUrl());
		}
		
		double width = m_camera.get(Videoio.CAP_PROP_FRAME_WIDTH);
		double height = m_camera.get(Videoio.CAP_PROP_FRAME_HEIGHT);
		m_resol = new Size(width, height);
		long sampleInterval = Math.round(1000.0 / m_config.getVideoConfig().getFps());
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("start capturing: camera={}, resol={}, fps={}, sample_interval={}ms",
							m_info.getId(), m_resol, m_config.getVideoConfig().getFps(), sampleInterval);
		}
		
		try ( OpenCvFrameSampler samples = new OpenCvFrameSampler(m_camera, sampleInterval);
				SampleFrameProcessor proc = new SampleFrameProcessor(this, m_noVideo); ) {
			samples.forEachOrThrow(proc);
		}
	}
	
	Tuple<File, VideoWriter> createVideoWriter(long startTs, float fps) {
		VideoConfig vconf = m_config.getVideoConfig();

		File camDir = new File(vconf.getVideoDir(), m_info.getId());
		camDir.mkdirs();
		LocalDateTime zdt = LocalDateTimes.fromEpochMillis(startTs);
		File videoFile = new File(camDir, String.format("%s.avi", zdt.format(FORMATTER)));
//		File videoFile = new File(camDir, String.format("%s.mp4", zdt.format(FORMATTER)));
		
		int fourcc = vconf.getFourcc();
		return Tuple.of(videoFile, new VideoWriter(videoFile.getAbsolutePath(), fourcc, fps, m_resol, true));
	}
}
