package dric.camera;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dric.topic.Topic;
import dric.type.CameraFrame;
import utils.func.CheckedConsumerX;
import utils.func.Tuple;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SampleFrameProcessor implements CheckedConsumerX<Tuple<Mat,Long>, SQLException>,
												AutoCloseable {
	private static final Logger s_logger = LoggerFactory.getLogger(SampleFrameProcessor.class);
	
	private final DrICCameraAgent m_agent;
	private final Topic<CameraFrame> m_topic;
	private final long m_videoInterval;
	private long m_startTs = -1;
	private long m_lastTs;
	private final MatOfByte m_mob = new MatOfByte();
//	private final FrameAppendSession m_appender;
	
	private File m_videoFile;
	private VideoWriter m_writer;
	private int m_appendCount = 0;
	
	SampleFrameProcessor(DrICCameraAgent agent) throws SQLException {
		m_agent = agent;
		m_topic = agent.getCameraFrameTopic();
		m_videoInterval = agent.getVideoTailInterval();
//		m_appender = new FrameAppendSession(m_agent.getJdbcProcessor());
	}

	@Override
	public void close() throws Exception {
//		m_appender.close();
		m_writer.release();
	}
	
	private void createVideoWriter(long startTs) {
		Tuple<File, VideoWriter> t = m_agent.createVideoWriter(m_startTs, m_agent.getFps());
		m_videoFile = t._1;
		m_writer = t._2;
		
		m_startTs = startTs;
		m_appendCount = 0;
	}
	
	@Override
	public void accept(Tuple<Mat,Long> sample) throws SQLException {
		long ts = sample._2;
		if ( m_startTs < 0 ) {
			m_startTs = ts;
			createVideoWriter(ts);
		}
		
		long elapsed = ts - m_startTs;
		if ( elapsed > m_videoInterval ) {
			m_writer.release();
			insertVideo(m_videoFile, m_startTs, m_lastTs);
			if ( s_logger.isInfoEnabled() ) {
				double fps = m_appendCount / ((m_lastTs - m_startTs + 1) / 1000.0);
				s_logger.info(String.format("actual fps=%.1f", fps));
			}
			
			createVideoWriter(ts);
		}
		m_writer.write(sample._1);
		
		Imgcodecs.imencode(".jpg", sample._1, m_mob);
		byte[] jpegBytes = m_mob.toArray();
		
		CameraFrame frame = new CameraFrame(m_agent.getCameraId(), jpegBytes, ts);
		m_topic.publish(frame);
		
//		m_appender.append(m_agent.getCameraId(), m_mob.toArray(), sample._2);
		m_lastTs = ts;
		++m_appendCount;
	}
	
	private void insertVideo(File file, long start, long stop) throws SQLException {
		String sql = "insert into camera_videos(camera_id, start_ts, stop_ts, file_path) "
					+ "values (?, ?, ?, ?)";
		JdbcProcessor jdbc = m_agent.getJdbcProcessor();
		try ( Connection conn = jdbc.connect(); ) {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, m_agent.getCameraId());
			pstmt.setLong(2, start);
			pstmt.setLong(3, stop);
			pstmt.setString(4, file.getAbsolutePath());
			
			pstmt.executeUpdate();
		}
	}
}
