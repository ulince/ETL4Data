package com.udl.test

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import com.udl.Downloader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.File;
import org.apache.hadoop.fs.Path;

public class DownloaderTest {

    @Test
    public void downloadTest(String path) {
        Downloader toTest = new Downloader();

        toTest.download(path);

        hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
        File file = new File("greater-london-latest.osm.bz2");
        Path filePath = new Path(path, "greater-london-latest.osm.bz2");
        assertTrue(file.exists());
    }

    @Test
    public void decompressTest(String path) {
        Downloader toTest = new Downloader();

        toTest.decompress(path);

        hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
        File file = new File("greater-london-latest.osm.bz2");
        Path filePath = new Path(path, "greater-london-latest.osm");
        assertTrue(file.exists());
    }

    @Test
    public void cleanupTest(String path) {
        Downloader toTest = new Downloader();

        toTest.decompress(path);

        hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
        File file = new File("greater-london-latest.osm.bz2");
        Path filePath = new Path(path, "greater-london-latest.osm.bz2");
        assertFalse(file.exists());
    }
}