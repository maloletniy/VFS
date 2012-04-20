package ru.mltny.vfs;

import ru.mltny.vfs.units.Node;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/15/12
 * Time: 5:36 PM
 * Interface of main library functionality
 */
public interface VFSTools {

    public void create(char type, String name, Node parent) throws Exception;

    public void write(Node node, byte[] bytes) throws Exception;

    public byte[] read(Node node) throws Exception;

    public void append(long node, byte[] bytes) throws Exception;

    public void rename(String path, String name) throws Exception;

    public void delete(String path) throws Exception;

    public void move(String path1, String path2) throws Exception;

    public Node getNodeByPath(long link) throws IOException;

}
