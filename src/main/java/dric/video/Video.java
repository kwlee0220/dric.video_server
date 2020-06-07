package dric.video;

import java.io.File;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoCapture;
import org.opencv.videoio.Videoio;

import dric.type.CameraFrame;
import utils.LocalDateTimes;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.FStreams.AbstractFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Video implements Comparable<Video> {
	private final String m_cameraId;
	private final long m_start;
	private final long m_stop;
	private final File m_videoFile;
	
	public Video(String cameraId, long start, long stop, File videoFile) {
		m_cameraId = cameraId;
		m_start = start;
		m_stop = stop;
		m_videoFile = videoFile;
	}
	
	public String cameraId() {
		return m_cameraId;
	}
	
	public long start() {
		return m_start;
	}
	
	public long stop() {
		return m_stop;
	}
	
	public File videoFile() {
		return m_videoFile;
	}
	
	public CameraFrame getFrame(long ts) throws FrameNotFoundException {
		return frames(ts, ts).findFirst()
							.getOrThrow(() -> new FrameNotFoundException(m_cameraId, ts));
	}
	
	public FStream<CameraFrame> frames() {
		return new CameraFrameStream(this, m_start, m_stop);
	}
	
	public FStream<CameraFrame> frames(long start, long stop) {
		return new CameraFrameStream(this, start, stop);
	}
	
	@Override
	public String toString() {
		return String.format("camera[%s -> %s (%s), file=%s]",
							LocalDateTimes.fromEpochMillis(m_start),
							LocalDateTimes.fromEpochMillis(m_stop),
							UnitUtils.toSecondString(m_stop - m_start + 1),
							m_videoFile);
	}

	@Override
	public int compareTo(Video o) {
		return Long.compare(m_start, o.m_start);
	}
	
	private static class CameraFrameStream extends AbstractFStream<CameraFrame> {
		private final String m_cameraId;
		private final VideoCapture m_cap;
		private final long m_videoStartTs;
		private final long m_start;
		private final long m_stop;
		
		
		private long m_cursorTs;
		private final Mat m_frame = new Mat();
		private final MatOfByte m_mob = new MatOfByte();
		
		private CameraFrameStream(Video video, long start, long stop) {
			m_cameraId = video.m_cameraId;
			m_videoStartTs = video.start();
			m_start = start;
			m_stop = stop;
			
			m_cap = new VideoCapture();
			m_cap.open(video.m_videoFile.getAbsolutePath());
			
			double ratio = (double)(start - m_start) / (m_stop - m_start + 1);
			int frameNo = (int)Math.floor(m_cap.get(Videoio.CAP_PROP_FRAME_COUNT) * ratio);
			m_cap.set(Videoio.CAP_PROP_POS_FRAMES, frameNo);
			
			m_cursorTs = 0;
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_cap.release();
		}

		@Override
		public FOption<CameraFrame> next() {
			while ( m_cap.read(m_frame) ) {
				m_cursorTs = (long)m_cap.get(Videoio.CAP_PROP_POS_MSEC) + m_videoStartTs;
				if ( m_cursorTs < m_start ) {
					continue;
				}
				if ( m_cursorTs > m_stop ) {
					return FOption.empty();
				}
				
				Imgcodecs.imencode(".jpg", m_frame, m_mob);
				byte[] image = m_mob.toArray();
				return FOption.of(new CameraFrame(m_cameraId, image, m_cursorTs));
			}
			return FOption.empty();
		}
	}
}
