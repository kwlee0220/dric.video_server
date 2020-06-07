package dric.camera;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FrameAppendSession implements AutoCloseable {
	private final Connection m_conn;
	private final PreparedStatement m_pstmt;
	
	private static final String INSERT_SQL = "INSERT INTO camera_frames values (?,?,?)";
	
	FrameAppendSession(JdbcProcessor jdbc) throws SQLException {
		m_conn = jdbc.connect();
		m_conn.setAutoCommit(true);
		
		m_pstmt = m_conn.prepareStatement(INSERT_SQL);
	}

	@Override
	public void close() throws SQLException {
		m_pstmt.close();
		m_conn.close();
	}
	
	public boolean append(String cctvId, byte[] image, long ts) throws SQLException {
		m_pstmt.setString(1, cctvId);
		m_pstmt.setLong(2, ts);
		m_pstmt.setBytes(3, image);
		
		return m_pstmt.executeUpdate() == 1;
	}
}
