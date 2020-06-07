package dric.video.grpc;

import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;

import dric.grpc.PBUtils;
import dric.proto.CameraFrameRangeRequest;
import dric.proto.CameraFrameRequest;
import dric.proto.CameraFrameResponse;
import dric.proto.CameraInfo;
import dric.proto.DrICVideoServerGrpc.DrICVideoServerImplBase;
import dric.type.CameraFrame;
import dric.video.CameraExistsException;
import dric.video.CameraNotFoundException;
import dric.video.DrICVideoServerImpl;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDrICVideoServerServant extends DrICVideoServerImplBase {
	private final DrICVideoServerImpl m_server;
	
	public PBDrICVideoServerServant(DrICVideoServerImpl server) {
		m_server = server;
	}
	
	@Override
    public void getCamera(StringValue id, StreamObserver<CameraInfo> out) {
		try {
			CameraInfo camera = m_server.getCamera(id.getValue());
			out.onNext(camera);
		}
		catch ( CameraNotFoundException e ) {
			out.onError(Status.NOT_FOUND
							.withDescription("CameraInfo is not found: id=" + id.getValue())
							.asException());
		}
		catch ( Exception e ) {
			out.onError(Status.INTERNAL
							.withDescription("cause=" + e)
							.asException());
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getCameraAll(Empty req, StreamObserver<CameraInfo> out) {
		try {
			m_server.getCameraAll()
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onError(Status.INTERNAL
							.withDescription("cause=" + e)
							.asException());
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void addCamera(CameraInfo info, StreamObserver<Empty> out) {
		try {
			m_server.addCamera(info);
			out.onNext(PBUtils.VOID);
		}
		catch ( CameraExistsException e ) {
			out.onError(Status.ALREADY_EXISTS
							.withDescription("CameraInfo exists: id=" + info.getId())
							.asException());
		}
		catch ( Exception e ) {
			out.onError(Status.INTERNAL
							.withDescription("cause=" + e)
							.asException());
		}
		finally {
			out.onCompleted();
		}
	}

	@Override
    public void removeCamera(StringValue id, StreamObserver<Empty> out) {
		try {
			m_server.removeCamera(id.getValue());
			out.onNext(PBUtils.VOID);
		}
		catch ( Exception e ) {
			out.onError(Status.INTERNAL
							.withDescription("cause=" + e)
							.asException());
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getCameraFrame(CameraFrameRequest req, StreamObserver<CameraFrameResponse> out) {
		try {
			CameraFrame frame = m_server.getCameraFrame(req.getCameraId(), req.getTs());
			out.onNext(toResponse(frame));
		}
		catch ( Exception e ) {
			out.onNext(toResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}

    public void queryCameraFrames(CameraFrameRangeRequest req, StreamObserver<CameraFrameResponse> out) {
		try {
			m_server.queryCameraFrames(req.getCameraId(), req.getStartTs(), req.getStopTs())
					.map(this::toResponse)
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onNext(toResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}
    
    private CameraFrameResponse toResponse(CameraFrame frame) {
    	return CameraFrameResponse.newBuilder()
								.setFrame(frame.toProto())
								.build();
    }
    
    private CameraFrameResponse toResponse(Exception e) {
    	return CameraFrameResponse.newBuilder()
								.setError(PBUtils.toErrorProto(e))
								.build();
    }
}
