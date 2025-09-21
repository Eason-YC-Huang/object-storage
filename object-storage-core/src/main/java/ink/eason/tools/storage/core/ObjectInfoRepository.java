package ink.eason.tools.storage.core;

import java.nio.file.Path;

public interface ObjectInfoRepository {

    public ObjectInfo getObjectInfo(String bucket, String key);

    public void saveObjectInfo(ObjectInfo objectInfo);

    public void deleteObjectInfo(ObjectInfo objectInfo);

    public ObjectInfo lastSavedObjectInfo();

}
