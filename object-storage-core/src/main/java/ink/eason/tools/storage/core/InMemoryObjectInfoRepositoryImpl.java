package ink.eason.tools.storage.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryObjectInfoRepositoryImpl implements ObjectInfoRepository {

    private final Map<String, AtomicLong> idGenerators = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ObjectInfo>> storage = new ConcurrentHashMap<>();
    private final AtomicReference<ObjectInfo> lastUploaded = new AtomicReference<>();

    @Override
    public ObjectInfo getObjectInfo(String bucket, String key) {
        Map<String, ObjectInfo> bucketObjects = storage.get(bucket);
        if (bucketObjects == null) {
            return null;
        }
        return bucketObjects.get(key);
    }

    @Override
    public void saveObjectInfo(ObjectInfo objectInfo) {
        AtomicLong idGenerator = idGenerators.computeIfAbsent(objectInfo.getBucket(), k -> new AtomicLong(0));

        objectInfo.setId(idGenerator.incrementAndGet());

        storage.computeIfAbsent(objectInfo.getBucket(), k -> new ConcurrentHashMap<>()).put(objectInfo.getKey(), objectInfo);
        lastUploaded.getAndUpdate((oldValue) -> {
            if (oldValue == null) {return  objectInfo;};
            return oldValue.getId() < objectInfo.getId() ? objectInfo : oldValue;
        });
    }

    @Override
    public void deleteObjectInfo(ObjectInfo objectInfo) {
        Map<String, ObjectInfo> bucketObjects = storage.get(objectInfo.getBucket());
        if (bucketObjects == null) {
            return;
        }
        bucketObjects.remove(objectInfo.getKey());
    }

    @Override
    public ObjectInfo lastSavedObjectInfo() {
        return null;
    }

}
