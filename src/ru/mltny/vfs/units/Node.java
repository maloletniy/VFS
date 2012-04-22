package ru.mltny.vfs.units;

import ru.mltny.vfs.Settings;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/18/12
 * Time: 1:34 PM
 * Main element of file system
 */
public class Node {
    private int size;
    private char[] name = new char[Settings.MAX_NAME_LENGTH];
    private char type;
    private long[] link = new long[Settings.MAX_DIR_COUNT];
    private long address;
    private long parentLink;

    public long getAddress() {
        return address;
    }

    public void setAddress(long address) {
        this.address = address;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public char[] getName() {
        return name;
    }

    public void setName(String name) {
        char[] arr = name.toCharArray();
        for (int i = 0; i < this.name.length; i++) {
            if (arr.length < i + 1) {
                this.name[i] = ' ';
            } else {
                this.name[i] = arr[i];
            }
        }
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public long[] getLink() {
        return link;
    }


    /**
     * writes type, name, links[], size about current node to file
     *
     * @param file container file descriptor
     * @throws IOException if any container problem
     */
    public void writeNode(RandomAccessFile file) throws IOException {
        file.seek(this.address);
        file.writeChar(this.type);
        file.writeChars(new String(this.name));
        for (long l : this.link) {
            file.writeLong(l);
        }
        file.writeInt(this.size);
    }

    /**
     * parse container content to node object
     *
     * @param file container descriptor
     * @param link pointer to node start
     * @return node object
     * @throws IOException if any container problem
     */
    public static Node getNodeAtPoint(RandomAccessFile file, long link) throws IOException {
        Node node = new Node();
        file.seek(link);
        node.setType(file.readChar());
        for (int i = 0; i < node.getName().length; i++) {
            node.getName()[i] = file.readChar();
        }

        for (int i = 0; i < 16; i++) {
            long l = file.readLong();
            node.getLink()[i] = l;
        }
        node.setSize(file.readInt());
        node.setAddress(link);
        return node;
    }

    /**
     * update node information about links in container
     *
     * @param file container descriptor
     * @throws IOException if any container problem
     */
    public void updateLinks(RandomAccessFile file) throws IOException {
        //смещение к указателя на ссылки
        file.seek(this.address + 34);
        for (long l : this.link) {
            file.writeLong(l);
        }
    }

    public void updateSize(RandomAccessFile file) throws IOException {
        //смещение к указателя на размер
        file.seek(this.address + 34 + 16 * 8);
        file.writeInt(this.size);
    }

    public void updateName(RandomAccessFile file) throws IOException {
        //смещение к указателя на имя
        file.seek(this.address + 2);
        for (char c : this.name) {
            file.writeChar(c);
        }
    }

    public static int getObjectSize() {
        //char char[16] long[32] int
        return 2 + 16 * 2 + 16 * 8 + 4;
    }

    @Override
    public String toString() {

        StringBuilder links = new StringBuilder();
        for (int i = 0; i < link.length; i++) {
            links.append(link[i]).append(" ");
        }
        return "Node{" + address + ": " +
                "size=" + size +
                ", name='" + new String(name).trim() +
                "', type=" + type +
                ", link=" + links.toString() +
                '}';
    }

}
