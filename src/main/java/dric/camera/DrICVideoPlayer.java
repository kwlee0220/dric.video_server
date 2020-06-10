package dric.camera;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoCapture;
import org.opencv.videoio.Videoio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.topic.Topic;
import dric.type.CameraFrame;
import utils.StopWatch;
import utils.func.CheckedRunnable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICVideoPlayer implements CheckedRunnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICVideoPlayer.class);
	
	private final String m_cameraId;
	private final String m_videoFile;
	private final Topic<CameraFrame> m_topic;
	
	private volatile VideoCapture m_camera;
	private volatile Size m_resol;
	private volatile float m_fps;
	
	public DrICVideoPlayer(String cameraId, String videoFile, float fps, Topic<CameraFrame> topic) {
		m_cameraId = cameraId;
		m_videoFile = videoFile;
		m_fps = fps;
		m_topic = topic;
	}
	
	String getCameraId() {
		return m_cameraId;
	}
	
	Size getResolution() {
		return m_resol;
	}
	
	float getFps() {
		return m_fps;
	}
	
	Topic<CameraFrame> getCameraFrameTopic() {
		return m_topic;
	}
	
	@Override
	public void run() throws Throwable {
		long start = System.currentTimeMillis();
		
		m_camera = new VideoCapture();
		if ( !m_camera.open(m_videoFile) ) {
			throw new IllegalArgumentException("fails to open camera: file=" + m_videoFile);
		}
		
		if ( m_fps == 0 ) {
			m_fps = (float)m_camera.get(Videoio.CAP_PROP_FPS);
		}
		double width = m_camera.get(Videoio.CAP_PROP_FRAME_WIDTH);
		double height = m_camera.get(Videoio.CAP_PROP_FRAME_HEIGHT);
		m_resol = new Size(width, height);
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("start capturing: camera={}:{}, resol={}, fps={}",
							m_cameraId, m_videoFile, m_resol, m_fps);
		}

		final Mat mat = new Mat();
		final MatOfByte bmat = new MatOfByte();
		long captureInterval = Math.round(1000 / m_fps);
		
		long elapsed = 0;
		try {
			while ( m_camera.isOpened() ) {
				long sleepMillis = Math.max(0, (captureInterval - elapsed));
//				System.out.println("sleep millis: " + sleepMillis);
				Thread.sleep(sleepMillis);
				
				StopWatch watch = StopWatch.start();
				if ( !m_camera.read(mat) ) {
					break;
				}
				
				Imgcodecs.imencode(".jpg", mat, bmat);
				byte[] jpegBytes = bmat.toArray();
				
				CameraFrame frame = new CameraFrame(m_cameraId, jpegBytes, System.currentTimeMillis());
				m_topic.publish(frame);
				elapsed = watch.stopInMillis();
			}
		}
		finally {
			m_camera.release();
		}
	}
}
