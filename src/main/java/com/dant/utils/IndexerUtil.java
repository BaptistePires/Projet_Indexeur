package com.dant.utils;

import javax.ws.rs.core.MultivaluedMap;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class IndexerUtil {
	/**
	 * Request header would be like :
	 * {
	 *    Content-Type=[image/png],
	 * 	  Content-Disposition=[form-data; name="file"; filename="filename.ext"]
	 * }
	 **/
	public static String getFileName(MultivaluedMap<String, String> header) {
		String[] contentDisposition = header.getFirst("Content-Disposition").split(";");
		for (String filename: contentDisposition) {
			if ((filename.trim().startsWith("filename"))) {
				String[] name = filename.split("=");
				String cleanName = name[1].trim().replaceAll("\"", "");
				return cleanName;
			}
		}
		return "unknown";
	}

	public static void saveFile(byte[] content, String filename) throws IOException {
		File file = new File(filename);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileOutputStream fop = new FileOutputStream(file);
		fop.write(content);
		fop.flush();
		fop.close();
	}

}
