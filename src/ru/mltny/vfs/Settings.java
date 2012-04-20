package ru.mltny.vfs;

/**
 * Created by IntelliJ IDEA.
 * User: maloletniy
 * Date: 4/18/12
 * Time: 1:59 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Settings {

    public static final short MAX_DIR_COUNT = 16;

    public static final short MAX_NAME_LENGTH = 16;

    public static final int CLUSTER_SIZE_BYTES = 1024;

    public static final int CLUSTER_COUNT = 100000;

    public static final int NODE_SIZE_BYTES = 166;

    public static final int NODE_COUNT = CLUSTER_COUNT / 4;

    public static final long CLUSTER_FIRST_ADDRESS = NODE_COUNT * NODE_SIZE_BYTES;

}
