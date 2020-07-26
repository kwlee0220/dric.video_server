package dric.video;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Iterables;

import dric.ConfigUtils;
import dric.proto.CameraInfo;
import dric.type.CameraFrame;
import utils.func.FOption;
import utils.func.Funcs;
import utils.func.Try;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrICVideoServerImpl implements DrICVideoServer {
	private final VideoServerConfig m_config;
	private final JdbcProcessor m_jdbc;
	
	public DrICVideoServerImpl(VideoServerConfig config) {
		m_config = config;
		m_jdbc = ConfigUtils.getJdbcProcessor(config.getJdbcEndPoint());
	}
	
	@Override
	public void addCamera(CameraInfo info) throws CameraExistsException, DrICVideoException {
		String sql = "insert into cameras(id, rtsp_url) values (?, ?)";
		
		Connection conn = null;
		try {
			conn = m_jdbc.connect();
			conn.setAutoCommit(false);
			
			if ( getCameraInfo(conn, info.getId()).isPresent() ) {
				throw new CameraExistsException(info.getId());
			}
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, info.getId());
			pstmt.setString(2, info.getRtspUrl());
			pstmt.executeUpdate();
			conn.commit();
		}
		catch ( SQLException e ) {
			try {
				conn.rollback();
			}
			catch ( SQLException e2 ) { }
			
			throw new DrICVideoException("" + e);
		}
		finally {
			if ( conn != null ) {
				try {
					conn.close();
				}
				catch ( SQLException e2 ) { }
			}
		}
	}
	
	@Override
	public void removeCamera(String id) throws DrICVideoException {
		String sql = "delete from cameras where id = ?";
		
		try {
			m_jdbc.executeUpdate(sql, pstmt -> {
				pstmt.setString(1, id);
			});
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
		catch ( ExecutionException e ) { }
	}

	@Override
	public CameraInfo getCamera(String cameraId) throws CameraNotFoundException, DrICVideoException {
		try ( Connection conn = m_jdbc.connect() ) {
			return getCameraInfo(conn, cameraId)
					.getOrThrow(() -> new CameraNotFoundException(cameraId));
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
	}

	@Override
	public FStream<CameraInfo> getCameraAll() throws DrICVideoException {
		try {
			return m_jdbc.streamQuery("select id, rtsp_url from cameras")
							.mapOrThrow(this::toCameraInfo);
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
	}
	
	public List<Video> queryVideos(String camId, long start, long stop) throws DrICVideoException {
		String sql = "select camera_id, start_ts, stop_ts, file_path "
					+ "from camera_videos "
					+ "where camera_id = ? "
					+ "and stop_ts >= ? and start_ts <= ? "
					+ "order by start_ts";
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, camId);
			pstmt.setLong(2, start);
			pstmt.setLong(3, stop);
			return m_jdbc.executeQuery(pstmt)
						.mapOrThrow(this::toVideo)
						.toList();
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
	}
	
	public CameraFrame getCameraFrame(String cameraId, long ts)
		throws FrameNotFoundException, DrICVideoException {
		try {
			Video video = getVideo(cameraId, ts);
			if ( video != null ) {
				return video.getFrame(ts);
			}
			else {
				return getTailFrame(cameraId, ts);
			}
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
	}

	public FStream<CameraFrame> queryCameraFrames(String cameraId, long start, long stop)
		throws DrICVideoException {
		try {
			List<Video> videoList = queryVideos(cameraId, start, stop);
			Video last = Iterables.getLast(videoList, null);
			long fileLast = Funcs.applyIfNotNull(last, v -> v.stop(), start-1);
			
			return FStream.from(videoList)
							.flatMap(video -> video.frames(start, stop))
							.concatWith(tailFrames(cameraId, fileLast+1, stop));
		}
		catch ( SQLException e ) {
			throw new DrICVideoException("" + e);
		}
	}
	
	private Video getVideo(String cameraId, long ts) throws SQLException {
		String sql = "select camera_id, start_ts, stop_ts, file_path "
					+ "from camera_videos "
					+ "where camera_id = ? "
					+ "and start_ts <= ? and stop_ts >= ?";
		
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, cameraId);
			pstmt.setLong(2, ts);
			pstmt.setLong(3, ts);
			return m_jdbc.executeQuery(pstmt)
							.mapOrThrow(this::toVideo)
							.findFirst()
							.getOrNull();
		}
	}
	
	private CameraFrame getTailFrame(String camId, long ts)
		throws SQLException, FrameNotFoundException {
		String sql = "select camera_id, ts, image from camera_frames "
					+ 	"where camera_id = ? and ts = ?";
		
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, camId);
			pstmt.setLong(2, ts);
			return m_jdbc.executeQuery(pstmt)
							.mapOrThrow(this::toFrame)
							.findFirst()
							.getOrThrow(() -> new FrameNotFoundException(camId, ts));
		}
	}
	
	private FStream<CameraFrame> tailFrames(String camId, long start, long stop) throws SQLException {
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(SQL_SELECT_FRAMES);
			pstmt.setString(1, camId);
			pstmt.setLong(2, start);
			pstmt.setLong(3, stop);
			return m_jdbc.executeQuery(pstmt)
							.mapOrThrow(this::toFrame);
		}
	}
	
	public static void format(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();
		Try.run(() -> stmt.executeUpdate("drop table camera_videos"));
		stmt.executeUpdate(SQL_CREATE_VIDEOS);
		Try.run(() -> stmt.executeUpdate("drop table cameras"));
		stmt.executeUpdate(SQL_CREATE_CAMERAS);
		Try.run(() -> stmt.executeUpdate("drop table camera_frames"));
		stmt.executeUpdate(SQL_CREATE_FRAMES);
	}
	
	private FOption<CameraInfo> getCameraInfo(Connection conn, String id) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement(SQL_SELECT_CAMERA);
		pstmt.setString(1, id);
		return m_jdbc.executeQuery(pstmt)
					.mapOrThrow(this::toCameraInfo)
					.findFirst();
	}
	
	private CameraInfo toCameraInfo(ResultSet rs) throws SQLException {
		return CameraInfo.newBuilder()
						.setId(rs.getString(1))
						.setRtspUrl(rs.getString(2))
						.build();
	}
	
	private Video toVideo(ResultSet rs) throws SQLException {
		return new Video(rs.getString(1), rs.getLong(2), rs.getLong(3),
								new File(rs.getString(4)));
	}
	
	private CameraFrame toFrame(ResultSet rs) throws SQLException {
		return new CameraFrame(rs.getString(1), rs.getBytes(3), rs.getLong(2));
	}

	private static final String SQL_CREATE_VIDEOS
		= "create table camera_videos ("
		+ 	"camera_id varchar not null,"
		+ 	"start_ts bigint not null,"
		+ 	"stop_ts bigint not null,"
		+ 	"file_path varchar not null,"
		+ 	"primary key (camera_id, start_ts)"
		+ ")";

	private static final String SQL_CREATE_FRAMES
		= "create table camera_frames ("
		+ 	"camera_id varchar not null,"
		+ 	"ts bigint not null,"
		+ 	"image bytea not null,"
		+ 	"primary key (camera_id, ts)"
		+ ")";

	private static final String SQL_CREATE_CAMERAS
		= "create table cameras ("
		+ 	"id varchar not null,"
		+ 	"rtsp_url varchar not null,"
		+ 	"primary key (id)"
		+ ")";
	
	private static final String SQL_SELECT_FRAMES
		= "select camera_id, ts, image from camera_frames "
		+ 	"where camera_id = ? and ts >= ? and ts <= ? "
		+ 	"order by ts";
	
	private static final String SQL_SELECT_CAMERA
		= "select id, rtsp_url from cameras where id = ?";
}
