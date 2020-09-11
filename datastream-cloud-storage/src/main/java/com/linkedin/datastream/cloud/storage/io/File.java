/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Package;


/**
 * File interface that should be implemented to support different file formats. Examples, SequenceFile, Parquet
 */
public interface File {
    static final Logger LOG = LoggerFactory.getLogger(File.class.getName());

    /**
     * returns the length of the file
     * @return length of the file
     * @throws IOException
     */
    long length() throws IOException;

    /**
     * writes
     * @param aPackage writes the record in the package to the file
     * @throws IOException
     */
    void write(Package aPackage) throws IOException;

    /**
     * closes the file
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * returns the path of the file
     * @return path of the file
     */
    String getPath();

    /**
     * returns the  of the file format
     * @return format/extension of the file
     */
    String getFileFormat();


    default boolean isCorrupt() {
        return false;
    }

    /**
     * delete the file
     */
    static void deleteFile(java.io.File file) {
        LOG.info("Deleting file {}", file.toPath());
        if (!file.delete()) {
            LOG.warn("Failed to delete file {}.", file.toPath());
        }

        // clean crc files
        final java.io.File crcFile = new java.io.File(file.getParent() + "/." + file.getName() + ".crc");
        if (crcFile.exists() && crcFile.isFile()) {
            if (!crcFile.delete()) {
                LOG.warn("Failed to delete crc file {}.", crcFile.toPath());
            }
        }
    }
}
