package ink.eason.tools.storage.core;

import java.io.InputStream;

public interface ObjectRepository {

    public void saveObject(String bucket, String key, String metadata, InputStream inputStream);

    public default void saveObject(String bucket, String key, InputStream inputStream){
        saveObject(bucket, key, null, inputStream);
    }

    public InputStream getObject(String bucket, String key);

    public void deleteObject(String bucket, String key);

}
