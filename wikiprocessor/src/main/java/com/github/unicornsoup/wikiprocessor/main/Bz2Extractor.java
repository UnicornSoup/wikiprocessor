package com.github.unicornsoup.wikiprocessor.main;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.log4j.Logger;

/**
 * 
 * @author anujagarwal464
 * This class extracts pages from .bz2 archive.
 * 
 */
public class Bz2Extractor {
	static final Logger log = Logger.getLogger(Bz2Extractor.class);

	public boolean extractPage(String archiveName) {
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(archiveName + ".bz2");
		} catch (FileNotFoundException e) {
			log.error("extractPage: File not found -- " + e.getMessage(), e);
			return false;
		}

		BufferedInputStream bin = new BufferedInputStream(fin);

		FileOutputStream out = null;
		try {
			out = new FileOutputStream(archiveName);
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
			return false;
		}

		BZip2CompressorInputStream bzIn = null;
		try {
			bzIn = new BZip2CompressorInputStream(bin);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return false;
		}

		final byte[] buffer = new byte[1024];

		int n = 0;
		try {
			while (-1 != (n = bzIn.read(buffer))) {
				out.write(buffer, 0, n);
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return false;
		}

		try {
			out.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return false;
		}
		try {
			bzIn.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return false;
		}

		return true;
	}
}
