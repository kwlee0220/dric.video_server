package dric.video.sunapi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import dric.proto.CameraInfo;
import dric.type.CameraFrame;
import dric.video.CameraExistsException;
import dric.video.CameraNotFoundException;
import dric.video.DrICVideoException;
import dric.video.DrICVideoServer;
import dric.video.FrameNotFoundException;
import dric.video.Video;
import utils.func.FOption;
import utils.func.KeyValue;
import utils.func.Tuple;
import utils.func.Tuple3;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SunApiVideoServerImpl implements DrICVideoServer {
	private final String m_host;
	private final int m_port;
	private final String m_userId;
	private final String m_passwd;
	private final HttpClientContext m_context;
	
	public SunApiVideoServerImpl(String host, int port, String userId, String passwd) {
		m_host = host;
		m_port = port;
		m_userId = userId;
		m_passwd = passwd;
		
	    CredentialsProvider cred = new BasicCredentialsProvider();
	    cred.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(m_userId, m_passwd));
	    AuthCache cache = new BasicAuthCache();
	    DigestScheme scheme = new DigestScheme();
	    scheme.overrideParamter("realm", "Wisenet NVR");
	    scheme.overrideParamter("nonce", "000000000000000000000000283026E7");
	    
	    HttpHost httpHost = new HttpHost(m_host, m_port, "http");
	    cache.put(httpHost, scheme);
	    
	    m_context = HttpClientContext.create();
	    m_context.setCredentialsProvider(cred);
	    m_context.setAuthCache(cache);
	}
	
	@Override
	public void addCamera(CameraInfo info) throws CameraExistsException, DrICVideoException {
		throw new DrICVideoException("fails to add the camera: id=" + info.getId());
	}
	
	@Override
	public void removeCamera(String id) throws DrICVideoException {
		throw new DrICVideoException("fails to remove the camera: id=" + id);
	}
	
	private static class SunApiCameraInfo {
		private final String m_cameraId;
		private final int m_channel;
		private final int m_profile;
		
		SunApiCameraInfo(String cameraId, int channel, int profile) {
			m_cameraId = cameraId;
			m_channel = channel;
			m_profile = profile;
		}
	}

	@Override
	public CameraInfo getCamera(String cameraId) throws CameraNotFoundException, DrICVideoException {
		try {
			Map<String,SunApiCameraInfo> infos = loadCameraInfos();
			SunApiCameraInfo info = infos.get(cameraId);
			if ( info == null ) {
				throw new CameraNotFoundException("camera.id=" + cameraId);
			}
			
			String urlTmpt = "http://%s:%d/stw-cgi/media.cgi?msubmenu=streamuri&action=view&Channel=%d"
						+ "&MediaType=Live&Mode=Full&ClientType=PC&StreamType=RTPUnicast&TransportProtocol=TCP"
						+ "&RTSPOverHTTP=False";
			String url = String.format(urlTmpt, m_host, m_port, info.m_channel);
			Tuple3<Integer,String,JsonObject> ret = callGetMethod(url);
			String streamUri = ret._3.get("URI").getAsString();
			String rtspUrl = String.format("%s%s:%s@%s", streamUri.substring(0, 7), m_userId, m_passwd,
																	streamUri.substring(7));
			
			return CameraInfo.newBuilder()
							.setId(cameraId)
							.setRtspUrl(rtspUrl)
							.build();
		}
		catch ( Exception e ) {
			throw new DrICVideoException(e);
		}
	}

	@Override
	public FStream<CameraInfo> getCameraAll() throws DrICVideoException {
		throw new UnsupportedOperationException();
	}
	
	public List<Video> queryVideos(String camId, long start, long stop) throws DrICVideoException {
		throw new UnsupportedOperationException();
	}
	
	public CameraFrame getCameraFrame(String cameraId, long ts)
		throws FrameNotFoundException, DrICVideoException {
		throw new UnsupportedOperationException();
	}

	public FStream<CameraFrame> queryCameraFrames(String cameraId, long start, long stop)
		throws DrICVideoException {
		throw new UnsupportedOperationException();
	}
	
	private Map<String,SunApiCameraInfo> loadCameraInfos() throws AuthenticationException, IOException {
		String url;
		Tuple3<Integer,String,JsonObject> ret;
		
		url = String.format("http://%s:%d/stw-cgi/media.cgi?msubmenu=videosource&action=view", m_host, m_port);
		ret = callGetMethod(url);
		Map<String,Integer> channels = parseCameraNameToChannelBindings(ret._3.getAsJsonArray("VideoSources"));
		
		url = String.format("http://%s:%d/stw-cgi/media.cgi?msubmenu=videoprofile&action=view", m_host, m_port);
		ret = callGetMethod(url);
		Map<Integer,Integer> profiles = parseChannelToProfileBindings(ret._3.getAsJsonArray("VideoProfiles"));
		
		return FStream.from(channels)
						.flatMapOption(kv -> {
							int profile = profiles.getOrDefault(kv.value(), -1);
							if ( profile >= 0 ) {
								return FOption.of(new SunApiCameraInfo(kv.key(), kv.value(), profile));
							}
							else {
								return FOption.empty();
							}
						})
						.toMap(info -> info.m_cameraId,  info -> info);
	}
	
	private static Map<String,Integer> parseCameraNameToChannelBindings(JsonArray sources) {
		return FStream.from(sources)
						.castSafely(JsonObject.class)
						.map(o -> KeyValue.of(o.get("Name").getAsString(), o.get("Channel").getAsInt()))
						.toMap(kv -> kv.key(), kv -> kv.value());
	}
	
	private static Map<Integer,Integer> parseChannelToProfileBindings(JsonArray profiles) {
		return FStream.from(profiles)
						.castSafely(JsonObject.class)
						.flatMapOption(o -> findDricProfile(o))
						.toMap(kv -> kv.key(), kv -> kv.value());
	}
	
	private static FOption<KeyValue<Integer,Integer>> findDricProfile(JsonObject channelProfiles) {
		int channel = channelProfiles.getAsJsonPrimitive("Channel").getAsInt();
		return FStream.from(channelProfiles.getAsJsonArray("Profiles"))
						.castSafely(JsonObject.class)
						.filter(p -> isDricProfile(p))
						.map(p -> p.get("Profile").getAsInt())
						.findFirst()
						.map(p -> KeyValue.of(channel, p));
	}
	
	private static boolean isDricProfile(JsonObject profile) {
		String name = profile.get("Name").getAsString();
		return name.equals("dric");
	}
	
	private Tuple3<Integer,String,JsonObject> callGetMethod(String url) throws IOException, AuthenticationException {
		try ( CloseableHttpClient client = HttpClients.createDefault(); ) {
			HttpGet httpGet = new HttpGet(url);
			
		    httpGet.setHeader("Content-type", "application/json");
		    httpGet.setHeader("Accept", "application/json");

		    CloseableHttpResponse resp = client.execute(httpGet, m_context);
	    	String details = String.format("%s(%d)", resp.getStatusLine().getReasonPhrase(),
														resp.getStatusLine().getStatusCode());
		    int code = resp.getStatusLine().getStatusCode();
		    if ( code >= 200 && code < 300 ) {
				JsonParser gson = new JsonParser();
			    ResponseHandler<String> handler = new BasicResponseHandler();
//			    System.out.println(handler.handleResponse(resp));
				JsonObject json = gson.parse(handler.handleResponse(resp)).getAsJsonObject();
			    
			    return Tuple.of(code, details, json);
		    }
		    else {
		    	return Tuple.of(code, details, (JsonObject)null);
		    }
		}
	}
}
