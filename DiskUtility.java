/**
 * Created by IntelliJ IDEA.
 * User: Alex Gris
 * Date: 12/9/13
 * Time: 9:23 PM
 */

package ro.alexandrugris.diskutil;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.lang.reflect.*;
import java.lang.annotation.*;

/**
 * Command line tool (designed as a file for easy compilation and start-up) for indexing HDDs,
 * checking for duplicate files and identifying large files
 * <p/>
 * Compile:
 * mkdir ./ro/alexandrugris/diskutil
 * mv ./DiskUtility.java ./ro/alexandrugris/diskutil/
 * javac ./ro/alexandrugris/diskutil/*.java
 * jar -cfm diskutil.jar Manifest.txt ./ro/alexandrugris/diskutil/*.class
 * <p/>
 * Run
 * derby.jar must be in the same directory
 * java -jar diskutil.jar
 */
public class DiskUtility {

    private static Connection c = null;


    @Retention(RetentionPolicy.RUNTIME) // Make this annotation accessible at runtime via reflection.
    @Target({ElementType.METHOD})       // This annotation can only be applied to class methods.
    public static @interface CmdLineParam {
        public String help() default "";
    }

    /**
     * Deletes a directory and all its subdirectories
     */
    private static class DeleteDirectoryAction {

        static PreparedStatement ps_findDirectory = null;
        static PreparedStatement ps_findDirectoryChildren = null;
        static PreparedStatement ps_deleteDirectoryChildren = null;
        static PreparedStatement ps_deleteFilesFromDirectory = null;
        static PreparedStatement ps_deleteDirectory = null;

        int countDeletes = 0;

        static {

            Connection c = DiskUtility.c;

            try {
                ps_findDirectory = c.prepareStatement("SELECT ID, DirPath, ParentID FROM Directories WHERE DirPath=?");
                ps_findDirectoryChildren = c.prepareStatement("SELECT ID, DirPath FROM Directories WHERE ParentID=?");
                ps_deleteDirectoryChildren = c.prepareStatement("DELETE FROM Directories WHERE ParentID=?");
                ps_deleteFilesFromDirectory = c.prepareStatement("DELETE FROM Files WHERE DirectoryRef=?");
                ps_deleteDirectory = c.prepareStatement("DELETE FROM Directories WHERE ID=?");
            } catch (SQLException e) {
                System.out.println(e);
            }
        }

        void deleteDirRecursively(int dirId, String dir_name) throws SQLException {

            countDeletes++;

            if (countDeletes % 200 == 0)
                System.out.print("/");
            if(countDeletes % 2000 == 0)
                System.out.println("");

            int affected_rows = 0;

            // delete all files from current directory
            ps_deleteFilesFromDirectory.setInt(1, dirId);
            ps_deleteFilesFromDirectory.executeUpdate();

            // recursively delete the rest of the directories
            ps_findDirectoryChildren.setInt(1, dirId);

            class Dir {
                public int _id;
                public String _name;

                public Dir(int id, String name) {
                    _id = id;
                    _name = name;
                }
            }

            Vector<Dir> v = new Vector<Dir>();

            try (ResultSet rs = ps_findDirectoryChildren.executeQuery()) {
                while (rs.next()) {
                    v.add(new Dir(rs.getInt(1), rs.getString(2)));
                }

            } catch (SQLException ex) {
                System.out.println(ex.toString());
                throw ex;
            }

            for (Dir d : v) {
                deleteDirRecursively(d._id, d._name);
            }

            // delete all the children
            ps_deleteDirectoryChildren.setInt(1, dirId);
            ps_deleteDirectoryChildren.executeUpdate();


            // delete the directory
            ps_deleteDirectory.setInt(1, dirId);
            affected_rows = ps_deleteDirectory.executeUpdate();

            if (affected_rows != 1)
                throw new SQLException("Something went wrong while deleting directories.");
        }

        /**
         * Returns the parent, if any
         *
         * @param p
         * @return
         * @throws IOException
         * @throws SQLException
         */

        public int findAndDeleteDirAndChildren(Path p) throws IOException, SQLException {

            int parent = -1;
            try {
                String path = p.toFile().getCanonicalPath();

                ResultSet rs = null;

                int dir = -1;
                String dirName = "";

                // find the directory to delete
                ps_findDirectory.setString(1, path);
                rs = ps_findDirectory.executeQuery();
                while (rs.next()) {
                    dir = rs.getInt(1);
                    dirName = rs.getString(2);

                    assert (parent == -1); // only one directory ;)
                    parent = rs.getInt(3);
                    if (rs.wasNull())
                        parent = -1;

                    // recursively delete the rest of the directories
                    deleteDirRecursively(dir, dirName);
                }

            } catch (SQLException e) {
                System.out.println(e.toString());
            }

            return parent;
        }

    }

    /**
     * Indexes a directory and all its subdirectories
     */
    private static class IndexDirectoryAction {

        private int nextDirID = 0;

        private long checkCommitFilesTime = 0;

        private static int getNextDirID() throws SQLException {
            int nextDirID = 0;
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT MAX(ID) From Directories");
            if (rs.next())
                nextDirID = rs.getInt(1) + 1;

            return nextDirID;
        }

        private void checkCommitFiles(PreparedStatement insert, long batchCount) throws SQLException {

            if (checkCommitFilesTime == 0)
                checkCommitFilesTime = System.currentTimeMillis();

            if (batchCount % 200 == 0) {
                insert.executeBatch();
                System.out.print("=");
            }

            if (batchCount % 2000 == 0) {

                c.commit();

                System.out.println("");
                System.out.println("Processed " + batchCount + " files [" + (System.currentTimeMillis() - checkCommitFilesTime) + "ms].");

                checkCommitFilesTime = System.currentTimeMillis();

            }
        }

        private long indexDirectory(Path dir, int dirId, int parentID, long batchCount) throws SQLException, IOException {

            ps_dirInsert.setInt(1, dirId);
            ps_dirInsert.setString(2, dir.toFile().getAbsolutePath());
            if (parentID != -1)
                ps_dirInsert.setInt(3, parentID);
            else
                ps_dirInsert.setNull(3, Types.INTEGER);
            // otherwise let it be null

            ps_dirInsert.executeUpdate();

            // for next directory;
            parentID = nextDirID;

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {

                for (Path p : ds) {
                    File f = p.toFile();

                    // second condition is to avoid links
                    if (!f.canRead() || f.getCanonicalPath().compareTo(f.getAbsolutePath()) != 0)
                        continue;

                    if (f.isDirectory()) {

                        // delete if already exists
                        c.commit();
                        dda.findAndDeleteDirAndChildren(p);
                        c.commit();
                        batchCount = indexDirectory(p, ++nextDirID, parentID, batchCount);
                    } else if(!f.getName().startsWith(".")){         // does not index hidden files

                        // add files as references to the directory

                        ps_insert.setInt(1, dirId);  // file path
                        ps_insert.setString(2, f.getName());          // file name
                        ps_insert.setLong(3, f.length());             // file size
                        ps_insert.setInt(4, f.getName().hashCode());            // hash code of file name

                        ps_insert.addBatch();
                        checkCommitFiles(ps_insert, ++batchCount);

                    }
                }
            } catch (IOException e) {
                System.out.println(e.toString());
            }

            return batchCount;
        }


        private static PreparedStatement ps_insert = null;
        private static PreparedStatement ps_dirInsert = null;

        DeleteDirectoryAction dda = new DeleteDirectoryAction();

        static {
            Connection c = DiskUtility.c;
            try {
                ps_insert = c.prepareStatement("INSERT INTO Files(DirectoryRef, FileName, Size, NameHash) VALUES (?, ?, ?, ?)");
                ps_dirInsert = c.prepareStatement("INSERT INTO Directories (ID, DirPath, ParentID) VALUES(?, ?, ?)");
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }

        public void updateDb(String dir) {

            long tm = System.currentTimeMillis();
            System.out.println("Indexing " + dir);

            try {

                Path p = FileSystems.getDefault().getPath(dir);

                int parentDir = dda.findAndDeleteDirAndChildren(p);

                nextDirID = getNextDirID();
                long files = indexDirectory(p, nextDirID, parentDir, 0);

                ps_insert.executeBatch();
                c.commit();

                System.out.println("");
                System.out.println("Total files in " + dir + ": " + files + ". [OK]");

            } catch (Exception e) {
                System.out.print(e.toString());
            }

            System.out.println("Duration: " + ((double) (System.currentTimeMillis() - tm)) / 1000.0 + "s.");
        }
    }

    private static class ListDuplicates {

        private static final int DO_NOTHING = 0;
        private static final int DELETE_FOLDER_RECURSIVE = 7;
        private static final int IGNORE_FOLDER_RECURSIVE = 8;

        private static final int DELETE_LEFT = 1;
        private static final int DELETE_ALL_LEFT = 2;
        private static final int SKIP_ALL_FILES_FROM_DIRECTORY_LEFT = 3;

        private static final int DELETE_RIGHT = 4;
        private static final int DELETE_ALL_RIGHT = 5;
        private static final int SKIP_ALL_FILES_FROM_DIRECTORY_RIGHT = 6;

        private static final int SKIP_ALL_FILES_FROM_DIRECTORY = 1007;
        private static final int DELETE_ALL_FROM_DIRECTORY = 1008;
        private static final int DELETE_ALL_FROM_DIRECTORY_RECURSIVELY = 1009;
        private static final int IGNORE_ALL_FROM_DIRECTORY_RECURSIVELY = 1010;

        private void deleteFileAndEmptyDir(String dir, String file) {
            try {
                File file_ = new File(dir + File.separator + file);
                file_.delete();

                File dir_ = new File(dir);
                if (dir_.list().length == 0)
                    dir_.delete();
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }

        private boolean fileExists(String dir, String file) {
            return (new File(dir + File.separator + file)).exists();
        }

        private boolean printOptionsMenu(Integer left_option, Integer right_option) {

            boolean smth_printed = false;

            if (left_option == null || left_option != SKIP_ALL_FILES_FROM_DIRECTORY) {
                smth_printed = true;
                System.out.println(DELETE_LEFT + ". Delete file from left");
                System.out.println(DELETE_ALL_LEFT + ". Delete all files from left directory");
                System.out.println(SKIP_ALL_FILES_FROM_DIRECTORY_LEFT + ". Ignore all comparisons with left directory");
            }

            if (right_option == null || right_option != SKIP_ALL_FILES_FROM_DIRECTORY) {
                smth_printed = true;
                System.out.println(DELETE_RIGHT + ". Delete file from right");
                System.out.println(DELETE_ALL_RIGHT + ". Delete all files from right directory");
                System.out.println(SKIP_ALL_FILES_FROM_DIRECTORY_RIGHT + ". Ignore all comparisons with right directory");
            }

            if (smth_printed) {

                System.out.println(DELETE_FOLDER_RECURSIVE + ". Delete recursive folder...");
                System.out.println(IGNORE_FOLDER_RECURSIVE + ". Ignore folder...");
                System.out.println(DO_NOTHING + ". Move to next file (do nothing)");

            }
            return smth_printed;
        }

        public void printDuplicates(long sizeKB, Vector<String> patterns) {

            /**
             * Always keeps the most recently used folder first for fast access
             */
            class FolderActions {

                class Pair<T1, T2> {
                    public Pair(T1 t1, T2 t2) {
                        fst = t1;
                        snd = t2;
                    }

                    public T1 fst;
                    public T2 snd;
                }

                private LinkedList<Pair<String, Integer>> folderActions = new LinkedList<>();

                public Integer get(String path) {

                    Integer ret = null;
                    int idx = 0; // to avoid constant switching between the first and the second

                    ListIterator<Pair<String, Integer>> it = folderActions.listIterator();
                    Pair<String, Integer> p = null;

                    while(it.hasNext()){
                        p = it.next();

                        if (p.snd == DELETE_ALL_FROM_DIRECTORY_RECURSIVELY && path.startsWith(p.fst)) {
                            ret = DELETE_ALL_FROM_DIRECTORY;
                            break;
                        }
                        else if (p.snd == IGNORE_ALL_FROM_DIRECTORY_RECURSIVELY && path.startsWith(p.fst)){
                            ret =  SKIP_ALL_FILES_FROM_DIRECTORY;
                            break;
                        } else if (path.equals(p.fst)) {
                            ret = p.snd;
                            break;
                        }

                        idx++;

                    }

                    if(p != null && idx > 2){
                        it.remove();
                        folderActions.addFirst(p);
                    }

                    return ret;
                }

                public String put(String s, int action) {

                    s = s.trim();
                    if (s.endsWith(File.separator))
                        s = s.substring(s.indexOf(File.separator));

                    if (!new File(s).exists() || !new File(s).isDirectory())
                        return null;

                    for (Pair<String, Integer> ps : folderActions) {
                        if (ps.fst.equals(s)) {
                            ps.snd = action;
                            return s;
                        }
                    }
                    folderActions.addFirst(new Pair<>(s, action));
                    return s;
                }
            }

            FolderActions folder_action = new FolderActions();

            String patternSearch = "";
            if (patterns.size() > 0) {
                patternSearch += " AND (f1.FileName LIKE '%." + patterns.elementAt(0);

                for (int i = 1; i < patterns.size(); i++)
                    patternSearch += "' OR f1.FileName LIKE '%." + patterns.elementAt(i);

                patternSearch += "')";
            }

            try (PreparedStatement ps_listDuplicates = c.prepareStatement(
                    "SELECT d1.DirPath, f1.FileName, d2.DirPath, f1.Size / (1024*1024), f1.ID, f2.ID, d1.ID, d2.ID FROM Directories d1, Files f1, Directories d2, Files F2 WHERE " +
                            "f1.Size > ? AND f1.Size=f2.Size AND f1.ID < f2.ID AND f1.FileName = f2.FileName AND d1.ID = f1.DirectoryRef AND d2.ID = f2.DirectoryRef" +
                            patternSearch
            )) {
                ps_listDuplicates.setLong(1, sizeKB * 1024);
                ResultSet rs = ps_listDuplicates.executeQuery();

                while (rs.next()) {

                    String folderLeft = rs.getString(1);
                    String folderRight = rs.getString(3);
                    String fileName = rs.getString(2);

                    if (!fileExists(folderLeft, fileName) ||
                        !fileExists(folderRight, fileName)) {
                        continue;
                    }

                    Integer left_option = folder_action.get(folderLeft);
                    Integer right_option = folder_action.get(folderRight);

                    if (left_option != null && left_option == DELETE_ALL_FROM_DIRECTORY) {
                        deleteFileAndEmptyDir(folderLeft, fileName);
                    } else if (right_option != null && right_option == DELETE_ALL_FROM_DIRECTORY) {
                        deleteFileAndEmptyDir(folderRight, fileName);
                    } else {

                        System.out.println("File: " + fileName + " [" + rs.getLong(4) + "MB]" + " f1.ID=" + rs.getLong(5) + " f2.ID=" + rs.getLong(6));
                        System.out.println(" --> " + folderLeft + " --> d1.ID = " + rs.getLong(7));
                        System.out.println(" --> " + folderRight + " --> d2.ID = " + rs.getLong(8));

                        if (printOptionsMenu(left_option, right_option))
                            try {

                                Scanner input = new Scanner(System.in);
                                int option = input.nextInt();

                                if (option == DO_NOTHING)
                                    continue;

                                switch (option) {

                                    case IGNORE_FOLDER_RECURSIVE:

                                        folder_action.put(input.nextLine(), IGNORE_ALL_FROM_DIRECTORY_RECURSIVELY);
                                        break;

                                    case DELETE_FOLDER_RECURSIVE:

                                        String folder = input.nextLine();
                                        folder = folder_action.put(folder, DELETE_ALL_FROM_DIRECTORY_RECURSIVELY);

                                        if (folder != null && folderLeft.startsWith(folder))
                                            deleteFileAndEmptyDir(folderLeft, fileName);
                                        else if (folder != null && folderRight.startsWith(folder))
                                            deleteFileAndEmptyDir(folderRight, fileName);

                                        break;

                                    case DELETE_ALL_LEFT:
                                        folder_action.put(folderLeft, DELETE_ALL_FROM_DIRECTORY);
                                    case DELETE_LEFT:
                                        deleteFileAndEmptyDir(folderLeft, fileName);
                                        break;
                                    case DELETE_ALL_RIGHT:
                                        folder_action.put(folderRight, DELETE_ALL_FROM_DIRECTORY);
                                    case DELETE_RIGHT:
                                        deleteFileAndEmptyDir(folderRight, fileName);
                                        break;
                                    case SKIP_ALL_FILES_FROM_DIRECTORY_LEFT:
                                        folder_action.put(folderLeft, SKIP_ALL_FILES_FROM_DIRECTORY);
                                        break;
                                    case SKIP_ALL_FILES_FROM_DIRECTORY_RIGHT:
                                        folder_action.put(folderRight, SKIP_ALL_FILES_FROM_DIRECTORY);
                                        break;
                                }

                            } catch (Exception ex) {
                                System.out.println(ex.toString());
                            }
                    }
                }


            } catch (SQLException ex) {
                 System.out.println(ex.toString());
            }
        }
    }

    static {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

            c = DriverManager.getConnection("jdbc:derby:AlexGrisDiskUtilityDb;create=true");
            c.setAutoCommit(false); // duration / file is twice with AutoCommit set to true.

            // check if tables are created
            try (Statement checkDb = c.createStatement()) {
                checkDb.executeQuery("SELECT MIN(ID) FROM Directories");
            } catch (SQLException e) {
                prepareDb(null);
            }

        } catch (Exception e) {
            System.out.println(e.toString());
            System.exit(0);
        }
    }

    @CmdLineParam(help = "Generate database structure: java DiskUtility --prepareDb")
    private static void prepareDb(Vector<String> noParam) throws SQLException {

        System.out.print("Creating database ... ");

        try (Statement s = c.createStatement()) {

            try {
                s.execute("DROP TABLE Files");
            } catch (Exception e) {
            }
            try {
                s.execute("DROP TABLE Directories");
            } catch (Exception e) {
            }

            s.execute("CREATE TABLE Directories(ID INT NOT NULL PRIMARY KEY, DirPath VARCHAR(1000) UNIQUE, ParentID INT REFERENCES Directories(ID))");
            s.execute("CREATE TABLE Files(ID INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY, DirectoryRef INT REFERENCES Directories(ID), FileName VARCHAR(1000), NameHash INT, Size BIGINT)");
            s.execute("CREATE INDEX FileNameHashIDX ON Files (NameHash)");
            s.execute("CREATE INDEX FileSizeIDX on Files(Size)");

        } catch (Exception e) {

            System.out.println(e.toString());
            throw new SQLException(e);

        }  // according to this, all references to other tables are silently dropped: http://db.apache.org/derby/docs/10.0/manuals/reference/sqlj28.html

        System.out.println("OK");
    }

    @CmdLineParam(help = "Full file database update: java DiskUtility --updateDb dir1 dir2 ...")
    public static void updateDb(Vector<String> directories) {

        IndexDirectoryAction ida = new IndexDirectoryAction();

        for (String dir : directories) {
            ida.updateDb(dir);
        }
    }

    @CmdLineParam(help = "--locate: Locates a set of files")
    public static void locate(Vector<String> files) {

        String sql = "SELECT d.DirPath, f.FileName FROM Directories d, Files f WHERE d.ID = f.DirectoryRef";

        for (String s : files) {
            sql += " AND LOWER(f.FileName) LIKE ?";
        }

        try (PreparedStatement ps_locateFile = c.prepareStatement(sql)) {

            int param = 0;
            for (String s : files) {
                s = s.toLowerCase();
                ps_locateFile.setString(++param, "%" + s + "%");
            }

            ResultSet rs = ps_locateFile.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString(1) + File.separator + rs.getString(2));
            }
        } catch (SQLException ex) {
            System.out.println(ex.toString());
        }

    }

    @CmdLineParam(help = "--duplicates [size] [type1] [type2]...: finds the duplicates.Example: --duplicates 1 mp3 avi docx mpg mp4 jpg png bmp jpeg")
    public static void duplicates(Vector<String> patterns) {

        long size = 0;
        ListDuplicates ld = new ListDuplicates();

        if (patterns.size() > 0) {
            try {
                size = Long.parseLong(patterns.elementAt(0));
                patterns.removeElementAt(0);
            } catch (NumberFormatException nolong) {
            }
        }

        ld.printDuplicates(size, patterns);
    }

    @CmdLineParam(help = "--usage [min_dir_size MB]")
    public static void usage(Vector<String> params) {

        int min_dir_size = 0;
        if (params.size() > 0) {
            min_dir_size = Integer.parseInt(params.elementAt(0));
        }

        String sql_all_files = "(SELECT SUM(Size) / (1024 * 1024) AS Total FROM Files)";

        String sql_filesize_dir = "SELECT file.Size, d.DirPath FROM " +
                "(SELECT SUM(f.Size) / (1024 * 1024) AS Size, f.DirectoryRef AS DirRef FROM Files f GROUP BY f.DirectoryRef) file, Directories d " +
                "WHERE file.DirRef = d.ID AND file.Size > ? ORDER BY file.Size DESC";


        try (
                PreparedStatement ps_all_files = c.prepareStatement(sql_all_files);
                PreparedStatement ps_dirs = c.prepareStatement(sql_filesize_dir);
        ) {

            ResultSet rs = ps_all_files.executeQuery();
            rs.next();
            long fullsize = rs.getLong(1);
            rs.close();

            ps_dirs.setInt(1, min_dir_size);
            rs = ps_dirs.executeQuery();

            long total_size = 0;

            while (rs.next()) {
                long dirSize = rs.getLong(1);
                System.out.println(rs.getString(2) + ": " + dirSize + "MB = " + (dirSize * 100) / fullsize + "%");
                total_size += dirSize;
            }

            System.out.println("Query covered " + (total_size * 100) / fullsize + "% out of all files.");

            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.toString());
        }
    }

    @CmdLineParam(help = "Show help: java DiskUtility --help")
    public static void help(Vector<String> p) {

        System.out.println("DiskUtility cmdline options: ");

        for (Method m : DiskUtility.class.getMethods()) {
            CmdLineParam param = m.getAnnotation(CmdLineParam.class);
            if (param != null) {
                System.out.println("  " + param.help());
            }
        }
    }

    /**
     * Run with --help for cmd line help
     *
     * @param args
     */
    public static void main(String[] args) {

        Method mth = null;
        Vector<String> params = new Vector<>();

        try {

            for (String arg : args) {

                if (arg.length() >= 2 && arg.charAt(0) == '-' && arg.charAt(1) == '-') {

                    if (mth != null) {
                        mth.invoke(null, params);
                        params = new Vector<>();
                    }

                    mth = DiskUtility.class.getMethod(arg.substring(2), Vector.class);

                } else if (mth != null && params != null) {
                    params.add(arg);
                }
            }

            if (mth != null)
                mth.invoke(null, params);

        } catch (NoSuchMethodException ex) {
            System.out.println(ex.toString());
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                try {
                    c.commit();
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
