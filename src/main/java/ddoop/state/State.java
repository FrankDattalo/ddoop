package ddoop.state;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class State implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(State.class);

  private static final String PATH_REGEX = "/([a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+";

  private final String dbPath;

  private DB db;

  private PathNode root;

  private Map<String, String> pathValues;

  public State(String dbPath) {
    this.dbPath = dbPath;
  }

  public synchronized void init() {

    logger.trace("init()");

    this.db = DBMaker.fileDB(this.dbPath)
        .fileMmapEnableIfSupported()
        .closeOnJvmShutdown()
        .transactionEnable()
        .executorEnable()
        .make();

    this.pathValues = this.db.hashMap("pathValues", Serializer.STRING, Serializer.STRING).createOrOpen();

    this.db.commit();

    this.root = new PathNode("/");

    logger.trace("init() done");
  }

  @Override
  public void close() {
    logger.trace("close()");

    if (this.db != null) {
      this.db.close();
    }
  }

  public synchronized List<Path> list(String path) {
    // if the path isn't the root directory, traverse to the directory
    // if the directory does not exist return an empty list
    // if the directory does exist return names of the files in that directory
    // if the path is a file, return just that file name

    logger.trace("list({})", path);

    checkPathFormat(path);

    logger.trace("path ok, traversing to destination");

    PathNode current = traverseTo(path, false);

    if (current == null) {
      logger.warn("destination does not exist");
      return Collections.emptyList();
    }

    if (!current.isDirectory()) {
      logger.trace("destination is file");
      return Collections.singletonList(Path.fromPathNode(current));
    }

    logger.trace("listing children");

    List<Path> ret = new ArrayList<>();

    for (PathNode pathNode : current.getChildren()) {
      ret.add(Path.fromPathNode(pathNode));
    }

    logger.trace("children: {}", ret);

    return ret;
  }

  public synchronized void delete(String path) {
    // if the path isn't the root directory, traverse to it
    // if the path does not exist return false
    // if the path does exist delete it and return true

    logger.trace("delete({})", path);

    checkPathFormat(path);

    PathNode current = traverseTo(path, false);

    if (current == null) {
      logger.warn("path does not exist");
      return;
    }

    logger.trace("recursively deleting path");

    current.delete();
  }

  public synchronized boolean isDirectory(String path) {

    checkPathFormat(path);

    PathNode node = traverseTo(path, false);

    return node != null && node.isDirectory();
  }

  public synchronized void put(String path, String value) {
    // if the path isnt the root directory, traverse to it

    checkPathFormat(path);

    PathNode node = traverseTo(path, true);

    if (node == null || node.getChildren().iterator().hasNext()) {
      logger.error("cannot put, this node is a directory");
      return;
    }

    logger.trace("set value of {}, to {}", node, value);

    node.setValue(value);
  }

  private PathNode traverseTo(String path, boolean create) {
    PathNode current = this.root;

    for (String pathName : parsePath(path)) {

      if (!current.isDirectory()) {
        logger.trace("Cannot traverse to {} from {}, current is a file", pathName, path);
        return null;
      }

      current = current.getChildByName(pathName, create);

      if (current == null) {
        logger.trace("cannot traverse to {} from {}, next was null", pathName, path);
        return null;
      }

    }

    return current;
  }

  private void checkPathFormat(String path) {
    if (!Pattern.matches(PATH_REGEX, path)) {
      throw new IllegalArgumentException("Illegal path format: " + path);
    }
  }

  private List<String> parsePath(String path) {
    List<String> ret = Arrays
        .stream(path.split("/"))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    logger.trace("parsePath({}) -> {}", path, ret);

    return ret;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    printContents(root, 0, 2, sb);
    return sb.toString();
  }

  private void printSpaces(StringBuilder sb, int indent) {
    for (int i = 0; i < indent; i++) {
      sb.append(' ');
    }
  }

  private void printContents(PathNode node, int currentIndent, int indent, StringBuilder sb) {

    logger.trace("printContents({}, {}, {})", node, currentIndent, indent);

    printSpaces(sb, currentIndent);

    sb.append(node.getName());

    if (!node.isDirectory()) {
      sb.append(" = ");
      sb.append(node.getValue());
      sb.append('\n');
      return;
    }

    sb.append('\n');
    int nextIndent = currentIndent + indent;

    for (PathNode nextChild : node.getChildren()) {
      printContents(nextChild, nextIndent, indent, sb);
    }
  }

  public static final class Path {
    private final String name;
    private final boolean isDirectory;
    private final String value;

    private static Path fromPathNode(PathNode pathNode) {
      return new Path(
          pathNode.getName(),
          pathNode.isDirectory(),
          pathNode.getValue()
      );
    }

    private Path(String name, boolean isDirectory, String value) {
      this.name = name;
      this.isDirectory = isDirectory;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public boolean isDirectory() {
      return isDirectory;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "Path{" +
          "name='" + name + '\'' +
          ", isDirectory=" + isDirectory +
          ", value='" + value + '\'' +
          '}';
    }
  }

  private final class PathNode {

    private final String absolutePath;

    public PathNode(String absolutePath) {
      this.absolutePath = absolutePath;
    }

    private void delete() {
      deleteChildren(this);

      deleteEmptyParents(this);

      State.this.db.commit();
    }

    private void deleteChildren(PathNode node) {

      State.this.pathValues.remove(node.absolutePath);

      for (PathNode child : node.getChildren()) {
        deleteChildren(child);
      }

      node.getChildrenList().clear();
    }

    private void deleteEmptyParents(PathNode node) {
      PathNode parent = node.getParent();

      if (parent == null) {
        return;
      }

      List<String> parentsChildren = parent.getChildrenList();

      parentsChildren.remove(node.absolutePath);

      if (parentsChildren.isEmpty()) {
        State.this.pathValues.remove(parent.absolutePath);
        deleteEmptyParents(node.getParent());
      }
    }

    private List<String> getChildrenList() {
      List<String> ret = State.this.db.indexTreeList(this.absolutePath, Serializer.STRING).createOrOpen();

      State.this.db.commit();

      return ret;
    }

    private void setValue(String value) {
      State.this.pathValues.put(this.absolutePath, value);

      State.this.db.commit();
    }

    private String basePath(String absolutePath) {
      String base = absolutePath.substring(basePathIndex(absolutePath));

      logger.trace("basePath({}) -> {}", absolutePath, base);

      return base;
    }

    private int basePathIndex(String absolutePath) {
      return absolutePath.lastIndexOf('/');
    }

    private PathNode getParent() {

      if (this.absolutePath.equals("/")) {
        return null;
      }

      if (this.absolutePath.lastIndexOf('/') == 0) {
        return root;
      }

      String parentPathName = this.absolutePath.substring(0, basePathIndex(this.absolutePath));

      logger.trace("parentPathName(absPath: {}) -> {}", this.absolutePath, parentPathName);

      return new PathNode(parentPathName);
    }

    private String getName() {

      if (this.absolutePath.equals("/")) {
        return "/";
      }

      return basePath(this.absolutePath);
    }

    private boolean isDirectory() {
      return !State.this.pathValues.containsKey(this.absolutePath);
    }

    private String getValue() {
      return State.this.pathValues.getOrDefault(this.absolutePath, null);
    }

    public PathNode getChildByName(String name, boolean create) {

      logger.trace("getChildByName({}, {})", name, create);

      String nextPathName = this.absolutePath.equals("/") ? "/" + name : this.absolutePath + "/" + name;

      List<String> childrenPaths = getChildrenList();

      logger.trace("children: {}", childrenPaths);

      for (String child : childrenPaths) {

        logger.trace("child: {}, name: {}", child, name);

        if (child.equals(nextPathName)) {
          return new PathNode(nextPathName);
        }
      }

      if (!create) {
        logger.trace("returning null, not creating path");
        return null;
      }

      childrenPaths.add(nextPathName);

      State.this.db.commit();

      return new PathNode(nextPathName);
    }

    public Iterable<PathNode> getChildren() {
      return getChildrenList().stream().map(PathNode::new).collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return absolutePath;
    }
  }

}
