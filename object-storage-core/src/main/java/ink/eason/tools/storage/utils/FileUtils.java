package ink.eason.tools.storage.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

public class FileUtils {

    public static final MessageDigest MD_5;

    static {
        try {
            MD_5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static long copy(InputStream in, Path out) throws IOException {
        return Files.copy(in, out);
    }

    public static Entry<Long, String> copyAndCalculateMd5(InputStream in, Path out) throws IOException {

        try (OutputStream fileOutputStream = Files.newOutputStream(out)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
                MD_5.update(buffer, 0, bytesRead);
            }
        }

        byte[] digest = MD_5.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return new SimpleEntry<>(Files.size(out), sb.toString());
    }

    public static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException ignore) {
        }
    }

}
