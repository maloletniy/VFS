package ru.mltny.vfs.units;

import ru.mltny.vfs.Settings;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/19/12
 * Time: 6:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class Cluster {
    private long link;
    private int size;
    private long address;

    public Cluster(long address) {
        this.address = address;
    }

    public Cluster(long link, int size, long address) {
        this.link = link;
        this.size = size;
        this.address = address;
    }

    public long getLink() {
        return link;
    }

    public void setLink(long link) {
        this.link = link;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getAddress() {
        return address;
    }

    public void setAddress(long address) {
        this.address = address;
    }

    /**
     * writes information about cluster to container
     *
     * @param file
     * @throws IOException
     */
    public void writeClusterInfo(RandomAccessFile file) throws IOException {
        file.seek(this.address);
        file.writeInt(this.size);
        file.seek(this.address + 4 + Settings.CLUSTER_SIZE_BYTES);
        file.writeLong(this.link);
    }

    /**
     * writes cluster data to container start from offset to cluster size or data.length(when it less then cluster size)
     *
     * @param file contatiner descriptor
     * @param data byte array of data
     * @param off  data array offset
     * @throws IOException
     */
    public void writeClusterData(RandomAccessFile file, byte[] data, int off) throws IOException {
        file.seek(this.address + 4);
        if (Settings.CLUSTER_SIZE_BYTES > (data.length - off)) {
            file.write(data, off, data.length - off);
        } else {
            file.write(data, off, Settings.CLUSTER_SIZE_BYTES);
        }
    }

    public static Cluster getClusterAtPoint(RandomAccessFile file, long link) throws IOException {
        file.seek(link);
        Cluster cluster = new Cluster(link);
        cluster.setSize(file.readInt());
        file.seek(cluster.getAddress() + 4 + Settings.CLUSTER_SIZE_BYTES);
        cluster.setLink(file.readLong());
        return cluster;
    }

    public int getClusterData(RandomAccessFile file, byte[] data, int off) throws IOException {
        //увказатель на данные
        file.seek(this.address + 4);
        return file.read(data, off, this.size);
    }

    public static int getObjectSize() {
        //long data[] long
        return 4 + Settings.CLUSTER_SIZE_BYTES + 8;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                this.address + ": " +
                "link=" + link +
                ", size=" + size +
                "}";
    }
}
