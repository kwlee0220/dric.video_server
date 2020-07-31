package dric.camera;


import java.io.File;
import java.util.List;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.type.CameraFrame;
import marmot.RecordWriteSession;
import marmot.dataset.DataSet;
import utils.StopWatch;
import utils.func.CheckedRunnable;
import utils.func.Try;
import utils.io.FileUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICImagePlayer implements CheckedRunnable {
	private static final Logger s_logger = LoggerFactory.getLogger(DrICImagePlayer.class);
	
	private final String m_cameraId;
	private final File m_imageDir;
	private final DataSet m_topic;
	private final float m_fps;
	
	public DrICImagePlayer(String cameraId, File imageDir, float fps, DataSet topic) {
		m_cameraId = cameraId;
		m_imageDir = imageDir;
		m_fps = fps;
		m_topic = topic;
	}
	
	String getCameraId() {
		return m_cameraId;
	}
	
	float getFps() {
		return m_fps;
	}
	
	@Override
	public void run() throws Throwable {
		long interval = Math.round(1000 / m_fps);
		
		List<File> imgFiles = FileUtils.walk(m_imageDir)
										.filter(File::isFile)
										.sort((f1, f2) -> f1.getName().compareTo(f2.getName()))
										.toList();
		
		try ( RecordWriteSession session = m_topic.openWriteSession() ) {
			final MatOfByte bmat = new MatOfByte();
			for ( File imgFile : imgFiles ) {
				StopWatch watch = StopWatch.start();
				
				Mat mat = Imgcodecs.imread(imgFile.getAbsolutePath(), Imgcodecs.IMREAD_UNCHANGED);
				Imgcodecs.imencode(".jpg", mat, bmat);
				byte[] jpegBytes = bmat.toArray();
				
				CameraFrame frame = new CameraFrame(m_cameraId, jpegBytes, System.currentTimeMillis());
				session.write(frame.toRecord());
				s_logger.debug("publish a image: " + imgFile);
				long remains = interval - watch.stopInMillis();
				
				if ( remains > 10 ) {
					Try.run(() -> Thread.sleep(remains));
				}
			}
		}
	}
}
