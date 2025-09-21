package ink.eason.tools.storage.core;

import ink.eason.tools.storage.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map.Entry;
import java.util.UUID;

public abstract class AbstractObjectStorageImpl implements ObjectStorage {

    protected final Path metaDir;
    protected final Path dataDir;
    protected final ObjectInfoRepository objectInfoRepository;

    protected final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    public AbstractObjectStorageImpl(Path metaDir, ObjectInfoRepository objectInfoRepository) {
        this(metaDir, null, objectInfoRepository);
    }

    public AbstractObjectStorageImpl(Path metaDir, Path dataDir , ObjectInfoRepository objectInfoRepository) {
        try {
            if (Files.notExists(metaDir)) {
                Files.createDirectories(metaDir);
            }
            dataDir = dataDir == null ? metaDir.resolve("data") : dataDir;
            if (Files.notExists(dataDir)) {
                Files.createDirectories(dataDir);
            }
            this.metaDir = metaDir;
            this.dataDir = dataDir;
            this.objectInfoRepository = objectInfoRepository;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static record AllocatedPath(String bucket, String key, Path physicalPath, String logicPath, long createTime) {}

    private AllocatedPath allocatePath(String bucket, String key) {
        Instant now = Instant.now();
        String date = now.atZone(ZoneOffset.UTC).format(dateFormatter);
        Path fileDir = dataDir.resolve(bucket).resolve(date);
        if (Files.notExists(fileDir)) {
            try {
                Files.createDirectories(fileDir);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        Path filePath;
        do {
            filePath = fileDir.resolve(UUID.randomUUID().toString());
        } while (Files.exists(filePath));
        return new AllocatedPath(bucket, key, filePath, dataDir.relativize(filePath).toString(), now.toEpochMilli());
    }

    @Override
    public InputStream getObject(String bucket, String key) {
        ObjectInfo objectInfo = objectInfoRepository.getObjectInfo(bucket, key);
        if (objectInfo == null) {
            return null;
        }
        try {
            return Files.newInputStream(dataDir.resolve(objectInfo.getPath()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void saveObject(String bucket, String key, String metadata, InputStream inputStream) {
        AllocatedPath allocatedPath = allocatePath(bucket, key);
        ObjectInfo objectInfo = new ObjectInfo();
        try {
            Entry<Long, String> result = FileUtils.copyAndCalculateMd5(inputStream, allocatedPath.physicalPath());
            objectInfo.setBucket(allocatedPath.bucket());
            objectInfo.setKey(allocatedPath.key());
            objectInfo.setPath(allocatedPath.logicPath());
            objectInfo.setCreateTime(allocatedPath.createTime());
            objectInfo.setSize(result.getKey());
            objectInfo.setMd5(result.getValue());
            objectInfo.setMetadata(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            objectInfoRepository.saveObjectInfo(objectInfo);
        } catch (Exception e) {
            FileUtils.delete(allocatedPath.physicalPath());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteObject(String bucket, String key) {

    }

    // === ObjectInfoOperations

    @Override
    public ObjectInfo getObjectInfo(String bucket, String key) {
        return objectInfoRepository.getObjectInfo(bucket, key);
    }

    @Override
    public void saveObjectInfo(ObjectInfo objectInfo) {
        objectInfoRepository.saveObjectInfo(objectInfo);
    }

    @Override
    public void deleteObjectInfo(ObjectInfo objectInfo) {
        objectInfoRepository.deleteObjectInfo(objectInfo);
    }

    @Override
    public ObjectInfo lastSavedObjectInfo() {
        return objectInfoRepository.lastSavedObjectInfo();
    }
}
