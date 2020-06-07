package opencvj;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OpenCvInitializer {
	private static final Logger s_logger = LoggerFactory.getLogger(OpenCvInitializer.class);
	private static final String DEF_OPENCV_DLL = "opencv_java430.dll";
//	private static final String DEF_OPENCV_DLL = "opencv_videoio_ffmpeg430_64.dll";
	
	public static void initialize() throws IOException {
		initialize(Lists.newArrayList(new File(new File("lib"), DEF_OPENCV_DLL)));
	}
	
	public static void initialize(List<File> dllFiles) throws IOException {
		for ( File dll: dllFiles ) {
			loadDll(dll);
		}
	}
	
	private static void loadDll(File dllFile) throws IOException {
		if ( dllFile.isFile() ) {
			String path = dllFile.getAbsolutePath();
			System.load(path);

			s_logger.debug("loaded file={}", path);
		}
		else {
			throw new IOException("invalid file path=" + dllFile.getAbsolutePath());
		}
	}
}
