import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.derby.shared.common.error.DerbySQLIntegrityConstraintViolationException;

public class DerbyTests {
    public String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    public String protocol = "jdbc:derby:";

    private Vector<String> FKTables = new Vector<String>();

    public static void main(String[] args) throws Exception {
        new DerbyTests().loadData();
    }

    public void loadData() throws Exception {
        Connection conn = DriverManager.getConnection(protocol + "derbyDB;create=true", null);
        File artistsFile = new File("artist.csv");
        FileReader fileReader = new FileReader(artistsFile);
        BufferedReader inputFileBuffer = new BufferedReader(fileReader);
        String readString = inputFileBuffer.readLine();
        String[] colNames = readString.replace("-", "").replace("2", "").replace("#", "").split("\",\"");
        inputFileBuffer.readLine();
        readString = inputFileBuffer.readLine();
        String[] colTypes = readString.split("\",\"");
        readString = inputFileBuffer.readLine();
        readString = inputFileBuffer.readLine();

        this.addArtistsTable(conn);
        final int[] labelFields = new int[] { 9, 11, 13, 15, 17, 19, 25, 29, 32, 34, 36, 38, 41, 44, 46, 48, 50, 52,
                55, 58, 60, 62, 65, 67, 71, 73, 77, 80, 83, 85, 87, 89, 91, 93, 95, 99, 101, 103, 105, 107,
                109, 111, 117, 119, 121, 123, 126, 130, 134 };

        for (int i : labelFields) {
            if (i == 8 || i == 24) {
                generateArtistFKTable1Field(conn, colNames[i]);
            } else {
                generateArtistFKTable2Field(conn, colNames[i]);
            }
        }
        // Remove 4 header lines from CSV.
        int i = 0;
        while (readString != null) {
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }
            String[] fields = readString.replace("'", "''").split("\",\"");
            int artistId = addArtistRecord(conn, fields, colNames);
            if (artistId < 0) {
                readString = inputFileBuffer.readLine();
                continue;
            }

            for (int j : labelFields) {
                if (colNames[j].contains("_label")) {
                    if (!fields[j].equals("NULL")) {
                        String[] labels = fields[j].replace("{", "").replace("}", "").split("\\|");
                        String[] uris = fields[j + 1].replace("{", "").replace("}", "").split("\\|");
                        for (int k = 0; k < labels.length; k++) {
                            addToArtistFKTable(conn, colNames[j], labels[k], uris[k]);
                        }
                    }
                } else if (fields[j].equals("NULL")) {
                    // Skip
                } else if (j == 8 || j == 24) {
                    // Alias and Birthname
                    String[] labels = fields[j].replace("{", "").replace("}", "").split("\\|");
                    for (int k = 0; k < labels.length; k++) {
                        addToArtistFKTable(conn, colNames[j], labels[k]);
                    }
                } else {
                    System.out.println("UNKNOWN SOMETHING " + colNames[j] + " " + colTypes[j]);
                }
            }
            readString = inputFileBuffer.readLine();
        }
        conn.close();
        inputFileBuffer.close();
    }

    private void updateArtistRecord(Connection conn, int artistId, String string, String string2) {
    }

    private Date parseDateFromCsv(String s) {
        try {
            DateFormat df = DateFormat.getDateInstance();
            if (s.contains("{")) { // Check if is an array
                // Remove array brackets and take first value of array
                s = s.substring(1, s.lastIndexOf("}"));
                s = s.split("\\|")[0];
            }
            if (s.equals("NULL") || s.contains("--")) { // Known null date formats.
                return null;
            } else if (s.contains("T") || s.contains(":")) {
                s = s.split("T")[0]; // Remove timestamp
                df = new SimpleDateFormat("yyyy-MM-dd");
            } else if (s.contains("-")) {
                df = new SimpleDateFormat("yyyy-MM-dd");
            } else if (s.contains("/")) {
                df = new SimpleDateFormat("dd/MM/yyyy");
            } else {
                System.err.println("Unknown date format for: " + s);
            }
            return df.parse(s);
        } catch (ParseException e) {
            System.err.println("Date parsing issue with date string: " + s);
            return null;
        }
    }

    public void addArtistsTable(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        try {
            st.execute("DROP TABLE ARTIST");
        } catch (Exception e) {
            //
        }
        try {
            st.execute("CREATE TABLE ARTIST (" +
                    "artistId INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, " +
                    "URI VARCHAR(255) UNIQUE, " +
                    "name VARCHAR(255), " +
                    "comment VARCHAR(2000), " +
                    "height FLOAT, " +
                    "weight FLOAT, " +
                    "runtime FLOAT, " +
                    "activeYearsEndYear INTEGER," +
                    "activeYearsStartYear INTEGER, " +
                    "background VARCHAR(255), " +
                    "bibsysId VARCHAR(25), " +
                    "birthDate DATE, birthName VARCHAR(255), birthYear INTEGER, " +
                    "bnfId VARCHAR(25), bpnId VARCHAR(25), deathDate DATE, deathYear INTEGER, " +
                    "individualisedGnd VARCHAR(25), isniId VARCHAR(25), lccnId VARCHAR(25), mbaId VARCHAR(255), " +
                    "networmbaidth FLOAT, nlaId VARCHAR(25), numberOfFilms INTEGER, orcidId VARCHAR(55) ," +
                    "pseudonym VARCHAR(255), recordDate DATE, restingPlacePosition VARCHAR(255), ridId VARCHAR(25), selibrId VARCHAR(25), "
                    +
                    "title VARCHAR(255), ulanId VARCHAR(25), viafId VARCHAR(25), wikiPageId INTEGER, wikiPageRevisionId INTEGER, description VARCHAR(1000), "
                    +
                    "point VARCHAR(255), posLat FLOAT, posLong FLOAT, " +
                    "PRIMARY KEY (artistID))");
        } catch (SQLException e) {

        }
    }

    private int addArtistRecord(Connection conn, String[] fields, String[] fieldNames) throws SQLException {
        HashMap<String, String> record = new HashMap<String, String>();
        // String fields
        record.put("URI", "'" + fields[0].replace("\"", "") + "'");
        record.put("name", "'" + fields[1] + "'");
        record.put("comment", "'" + fields[2] + "'");
        final int[] stringFields = new int[] { 21, 22, 24, 28, 30, 31, 57, 64, 69, 70, 75, 76, 82, 97, 113,
                114, 116, 125, 128, 129, 137 };
        final int[] yearFields = new int[] { 6, 7, 27, 43 };
        final int[] dateFields = new int[] { 23, 40, 98 };
        final int[] numberFields = new int[] { 54, 75, 115, 132, 133, 136 };
        for (int i : stringFields) {
            if (!fields[i].equals("NULL")) {
                record.put(fieldNames[i], "'" + fields[i] + "'");
            }
        }
        for (int i : yearFields) {
            if (!fields[i].equals("NULL")) {
                if (parseDateFromCsv(fields[i]) != null) {
                    String dateString = new SimpleDateFormat("yyyy").format(parseDateFromCsv(fields[i]));
                    record.put(fieldNames[i], dateString);
                }
            }
        }
        for (int i : dateFields) {
            if (parseDateFromCsv(fields[i]) != null && !fields[i].contains("--")) {
                String dateString = new SimpleDateFormat("''yyyy-MM-dd''").format(parseDateFromCsv(fields[i]));
                record.put(fieldNames[i], dateString);
            }
        }
        for (int i : numberFields) {
            if (!fields[i].equals("NULL")) {
                record.put(fieldNames[i], fields[i]);
            }
        }
        if (!fields[143].equals("NULL")) {
            record.put("posLat", fields[143]);
        }
        if (!fields[144].equals("NULL")) {
            record.put("posLong", fields[144]);
        }
        if (!fields[79].equals("NULL")) {
            if (fields[79].contains("{")) {
                int numFilms = 0;
                for (String s : fields[79].replace("{", "").replace("}", "").split("\\|")) {
                    numFilms += Integer.parseInt(s);
                }
                fields[79] = String.format("%s", numFilms);
            }
            record.put(fieldNames[79], fields[79]);
        }
        Statement st;
        st = conn.createStatement();
        try {
            String sql = String.format("INSERT INTO ARTIST (%s) VALUES (%s)", String.join(",", record.keySet()),
                    String.join(",", record.values()));
            st.execute(sql);
            st.executeQuery(String.format("SELECT * FROM ARTIST WHERE (URI = '%s')", fields[0].replace("\"", "")));
            ResultSet rs = st.getResultSet();
            rs.next();
            return rs.getInt("artistId");
        } catch (DerbySQLIntegrityConstraintViolationException e) {

        }
        return -1;
    }

    private void addToArtistFKTable(Connection conn, String tableName, String field1) throws SQLException {
        Runnable runnable = () -> {
            Connection _conn = null;
            try {
                _conn = DriverManager.getConnection(protocol + "derbyDB;create=true", null);
                Statement st = _conn.createStatement();
                st.execute(String.format("INSERT INTO %s (field1) VALUES ('%s')", tableName, field1));
            } catch (SQLException e) {
            }
            try {
                _conn.close();
            } catch (Exception e) {
            }
        };
        runnable.run();

    }

    private void addToArtistFKTable(Connection conn, String tableName, String field1, String field2)
            throws SQLException {
        Runnable runnable = () -> {
            Connection _conn = null;
            try {
                _conn = DriverManager.getConnection(protocol + "derbyDB;create=true", null);
                Statement st = _conn.createStatement();
                st.execute(String.format("INSERT INTO %s (field1, field2) VALUES ('%s', '%s')", tableName, field1,
                        field2));
            } catch (SQLException e) {
            }
            try {
                _conn.close();
            } catch (Exception e) {
            }
        };
        runnable.run();
    }

    private void generateArtistFKTable1Field(Connection conn, String tableName)
            throws SQLException {
        Statement st;

        st = conn.createStatement();
        try {
            st.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {

        }
        st.execute(String.format(
                "CREATE TABLE %s (rId INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT %srpk PRIMARY KEY, " +
                        "artistId INT CONSTRAINT %sartistId REFERENCES ARTIST ON DELETE CASCADE, field1 VARCHAR(255))",
                tableName, tableName, tableName));

    }

    private void generateArtistFKTable2Field(Connection conn, String tableName)
            throws SQLException {
        Statement st;

        st = conn.createStatement();
        try {
            st.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {

        }
        st.execute(String.format(
                "CREATE TABLE %s (rId INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT %srpk PRIMARY KEY, " +
                        "artistId INT CONSTRAINT %sartistId REFERENCES ARTIST ON DELETE CASCADE, field1 VARCHAR(255), field2 VARCHAR(255))",
                tableName, tableName, tableName));

    }
}
