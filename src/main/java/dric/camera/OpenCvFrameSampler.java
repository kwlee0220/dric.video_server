package dric.camera;

import org.opencv.core.Mat;
import org.opencv.videoio.VideoCapture;

import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStreams.AbstractFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OpenCvFrameSampler extends AbstractFStream<Tuple<Mat,Long>> {
	private final VideoCapture m_camera;
	private long m_due = -1;
	private long m_interval;
	
	private final Mat m_frame = new Mat();
	
	OpenCvFrameSampler(VideoCapture camera, long interval) {
		Utilities.checkNotNullArgument(camera);
		Utilities.checkArgument(interval > 0, "invalid sample interval: " + interval);
		
		m_camera = camera;
		m_interval = interval;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_camera.release();
	}

	@Override
	public FOption<Tuple<Mat,Long>> next() {
		while ( m_camera.isOpened() ) {
			if ( !m_camera.read(m_frame) ) {
				throw new IllegalArgumentException("fails to capture a frame from camera: " + m_camera);
			}
			
			long ts = System.currentTimeMillis();
//			System.out.printf("gap=%d%n", m_due-ts);
			if ( ts >= (m_due-20) ) {
				m_due = ts + m_interval;
				return FOption.of(Tuple.of(m_frame, ts));
			}
//			else {
//				System.out.println("SKIP!!!");
//			}
		}
		
		return FOption.empty();
	}
}
