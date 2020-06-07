package dric.camera;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.LocalDateTimes;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.Tuple;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VideoCreater implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(VideoCreater.class);
	
	private final DrICCameraAgent m_agent;
	private final float m_fps;
	private final long m_startTs;
	private final long m_stopTs;
	
	VideoCreater(DrICCameraAgent agent, long startTs, long stopTs, float fps) {
		m_agent = agent;
		m_fps = fps;
		m_startTs = startTs;
		m_stopTs = stopTs;
	}

	@Override
	public void run() {
		Tuple<File, VideoWriter> t = m_agent.createVideoWriter(m_startTs, m_fps);
		
		StopWatch watch = StopWatch.start();
		if ( s_logger.isDebugEnabled() ) {
			String msg = String.format("creating a video file: file=%s, fps=%.1f", t._1, m_fps);
			s_logger.debug(msg);
		}
		
		VideoWriter vwriter = t._2;
		JdbcProcessor jdbc = m_agent.getJdbcProcessor();
		try ( Connection conn = jdbc.connect(); ) {
			PreparedStatement pstmt = conn.prepareStatement(SQL_SELECT_FRAMES);
			pstmt.setString(1, m_agent.getCameraId());
			pstmt.setLong(2, m_startTs);
			pstmt.setLong(3, m_stopTs);
			
			jdbc.executeQuery(pstmt)
				.mapOrThrow(this::toFrame)
				.forEachOrThrow(vwriter::write);
			insertVideo(t._1);
			
			if ( s_logger.isInfoEnabled() ) {
				String startTime = LocalDateTimes.fromEpochMillis(m_startTs).toString();
				String durStr = UnitUtils.toSecondString(m_stopTs - m_startTs + 1);
				String msg = String.format("created a video file: file=%s, time=%s:%s, fps=%.1f, elapsed=%s",
											t._1, startTime, durStr, m_fps,
											watch.stopAndGetElpasedTimeString());
				s_logger.info(msg);
			}
		}
		catch ( Exception e ) {
			e.printStackTrace(System.err);
		}
		finally {
			vwriter.release();
			if ( s_logger.isInfoEnabled() ) {
				String length = UnitUtils.toSecondString(m_agent.getVideoTailInterval());
				s_logger.info("created: video file={} length={}", t._1, length);
			}
			
			try ( Connection conn = jdbc.connect(); ) {
				PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_FRAMES);
				pstmt.setString(1, m_agent.getCameraId());
				pstmt.setLong(2, m_startTs);
				pstmt.setLong(3, m_stopTs);
				pstmt.executeUpdate();
			}
			catch ( Exception ignored ) { }
		}
	}
	
	public void insertVideo(File file) throws SQLException {
		JdbcProcessor jdbc = m_agent.getJdbcProcessor();
		try ( Connection conn = jdbc.connect(); ) {
			PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_VIDEO);
			pstmt.setString(1, m_agent.getCameraId());
			pstmt.setLong(2, m_startTs);
			pstmt.setLong(3, m_stopTs);
			pstmt.setString(4, file.getAbsolutePath());
			
			pstmt.executeUpdate();
		}
	}
	
	private Mat toFrame(ResultSet rs) throws SQLException {
		byte[] image = rs.getBytes(1);
		return Imgcodecs.imdecode(new MatOfByte(image), Imgcodecs.IMREAD_UNCHANGED);
	}
	
	private static final String SQL_SELECT_FRAMES
		= "select image from camera_frames "
		+ "where camera_id = ? "
		+ "and ts >= ? and ts <= ?";
	
	private static final String SQL_DELETE_FRAMES
		= "delete from camera_frames "
		+ "where camera_id = ? "
		+ "and ts >= ? and ts <= ?";

	private static final String SQL_INSERT_VIDEO
		= "insert into camera_videos(camera_id, start_ts, stop_ts, file_path) "
		+ "values (?, ?, ?, ?)";
}
