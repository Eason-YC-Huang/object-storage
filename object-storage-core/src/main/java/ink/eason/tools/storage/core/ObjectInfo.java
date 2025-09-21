package ink.eason.tools.storage.core;

import java.util.Objects;

public class ObjectInfo {
    private Long id;
    private String bucket;
    private String key;
    private String path;
    private long createTime;
    private long size;
    private String md5;
    private String metadata;

    @Override
    public String toString() {
        return "ObjectInfo{" +
                "id=" + id +
                ", path='" + path + '\'' +
                ", createTime=" + createTime +
                ", bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                ", size=" + size +
                ", md5='" + md5 + '\'' +
                ", metadata='" + metadata + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ObjectInfo that = (ObjectInfo) o;
        return createTime == that.createTime && size == that.size && Objects.equals(id, that.id) && Objects.equals(path, that.path) && Objects.equals(bucket, that.bucket) && Objects.equals(key, that.key) && Objects.equals(md5, that.md5) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path, createTime, bucket, key, size, md5, metadata);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
