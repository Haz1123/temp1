import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DerbyTests {
    public String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    public String protocol = "jdbc:derby:";
    private File artistsDerbyCSV = new File("artist_derby.csv");
    private FileWriter artistsFileWriter;
    private HashMap<String, FileWriter> fkTableCSVs;
    private HashSet<String> uniqueRecords;

    public static void main(String[] args) throws Exception {
        DerbyTests dt = new DerbyTests();
        dt.makeCSVs();
        dt.loadCSVs();
        // Post impressionists -
        String aaa = "SELECT ARTIST.URI, ARTIST.NAME,MOVEMENT_LABEL.FIELD1 FROM MOVEMENT_LABEL JOIN ARTIST ON ARTIST.URI = MOVEMENT_LABEL.ARTISTURI AND MOVEMENT_LABEL.FIELD1 LIKE '%Post-Impressionism%';";
        // punk rock australian born
        String bbb = "SELECT ARTIST.URI, ARTIST.NAME, GENRE_LABEL.FIELD1, BIRTHPLACE_LABEL.FIELD1 FROM GENRE_LABEL JOIN ARTIST ON ARTIST.URI = GENRE_LABEL.ARTISTURI AND GENRE_LABEL.FIELD1 LIKE '%Punk rock%' JOIN BIRTHPLACE_LABEL ON ARTIST.URI = BIRTHPLACE_LABEL.ARTISTURI AND BIRTHPLACE_LABEL.FIELD1 LIKE '%Australia%';";
    }

    private void loadCSVs() throws SQLException {
        System.out.println("Loading data from Artists processed CSV");
        Connection conn = DriverManager.getConnection(protocol + "derbyDB;create=true", null);
        Statement st = conn.createStatement();
        st.execute("TRUNCATE TABLE ARTIST");
        st.execute("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (null,'ARTIST','artist_derby.csv',',',null,null,0)");
        st.execute("SELECT COUNT(*) c FROM ARTIST");
        ResultSet rs = st.getResultSet();
        if (rs.next()) {
            System.out.println("Records read: " + rs.getString("c"));
        }
        for (String fkTableName : this.fkTableCSVs.keySet()) {
            System.out.println(fkTableName);
            if (!fkTableName.equals("pseudonym") && !fkTableName.equals("alias") && !fkTableName.equals("birthName")) {
                st.execute("CALL SYSCS_UTIL.SYSCS_IMPORT_DATA(null,'" + fkTableName.toUpperCase()
                        + "', 'ARTISTURI ,FIELD1 ,FIELD2', null, '" + "csv/"
                        + fkTableName + "derby.csv',',',null,null,0)");
            } else {
                st.execute("CALL SYSCS_UTIL.SYSCS_IMPORT_DATA(null,'" + fkTableName.toUpperCase()
                        + "', 'ARTISTURI ,FIELD1', null, '" + "csv/"
                        + fkTableName + "derby.csv',',',null,null,0)");
            }

        }
        conn.close();

    }

    public void makeCSVs() throws Exception {
        // File setups
        System.out.println("Building Tables");
        Connection conn = DriverManager.getConnection(protocol + "derbyDB;create=true", null);
        File artistsFile = new File("artist.csv");
        FileReader fileReader = new FileReader(artistsFile);
        BufferedReader inputFileBuffer = new BufferedReader(fileReader);
        this.artistsFileWriter = new FileWriter(artistsDerbyCSV);
        this.fkTableCSVs = new HashMap<String, FileWriter>();
        // Read in columns
        String readString = inputFileBuffer.readLine();
        String[] colNames = readString.replace("-", "").replace("2", "").replace("#", "").split("\",\"");
        colNames[0] = "URI";
        colNames[1] = "name";
        colNames[2] = "comment";
        inputFileBuffer.readLine();
        readString = inputFileBuffer.readLine();
        String[] colTypes = readString.split("\",\"");
        readString = inputFileBuffer.readLine();
        readString = inputFileBuffer.readLine();
        this.addArtistsTable(conn);
        final int[] labelFields = new int[] { 8, 24, 97, 9, 11, 13, 15, 17, 19, 25, 29, 32, 34, 36, 38, 41, 44, 46, 48,
                50, 52, 55, 58, 60, 62, 65, 67, 71, 73, 77, 80, 83, 85, 87, 89, 91, 93, 95, 99, 101, 103, 105, 107,
                109, 111, 117, 119, 121, 123, 126, 130, 134 };

        for (int i : labelFields) {
            if (i == 8 || i == 24 || i == 97) {
                generateArtistFKTable1Field(conn, colNames[i]);
            } else {
                generateArtistFKTable2Field(conn, colNames[i]);
            }
        }
        System.out.println("Writing CSVs");
        this.uniqueRecords = new HashSet<String>();
        // Remove 4 header lines from CSV.
        int i = 0;
        while (readString != null) {
            i++;
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            String[] fields = readString.replace("'", "''").split("\",\"");
            int res = addArtistRecord(conn, fields, colNames, artistsFileWriter);
            if (res == -1) {
                readString = inputFileBuffer.readLine();
                continue;
            }
            for (int j : labelFields) {
                if (colNames[j].contains("_label")) {
                    if (!fields[j].equals("NULL")) {
                        String[] labels = fields[j].replace("{", "").replace("}", "").split("\\|");
                        String[] uris = fields[j + 1].replace("{", "").replace("}", "").split("\\|");
                        for (int k = 0; k < labels.length; k++) {
                            addToArtistFKTable(conn, colNames[j], fields[0].replace("\"", ""), labels[k],
                                    uris[k]);
                        }
                    }
                } else if (fields[j].equals("NULL")) {
                    // Skip
                } else if (j == 8 || j == 24 || j == 97) {
                    // Alias, birthname and pseudonym
                    String[] labels = fields[j].replace("{", "").replace("}", "").split("\\|");
                    for (int k = 0; k < labels.length; k++) {
                        addToArtistFKTable(conn, colNames[j], fields[0].replace("\"", ""), labels[k]);
                    }
                } else {
                    System.out.println("UNKNOWN SOMETHING " + colNames[j] + " " + colTypes[j]);
                }
            }
            readString = inputFileBuffer.readLine();
        }
        this.artistsFileWriter.close();
        conn.close();
        inputFileBuffer.close();
        for (FileWriter fw : this.fkTableCSVs.values()) {
            fw.close();
        }
        System.out.println("Wrote records to csvs");
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
            System.out.println("Unable to drop artist table.");
        }
        try {
            st.execute("CREATE TABLE ARTIST (" +
                    "URI VARCHAR(255), " +
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
                    "individualisedGnd VARCHAR(25), isniId VARCHAR(50), lccnId VARCHAR(50), mbaId VARCHAR(255), " +
                    "networmbaidth FLOAT, nlaId VARCHAR(25), numberOfFilms INTEGER, orcidId VARCHAR(55) ," +
                    "recordDate DATE, restingPlacePosition VARCHAR(255), ridId VARCHAR(50), selibrId VARCHAR(50), "
                    +
                    "title VARCHAR(255), ulanId VARCHAR(50), viafId VARCHAR(50), wikiPageId INTEGER, wikiPageRevisionId INTEGER, description VARCHAR(1000), "
                    +
                    "point VARCHAR(255), posLat FLOAT, posLong FLOAT, " +
                    "PRIMARY KEY (URI))");
        } catch (SQLException e) {
            System.out.println("Unable to make artist table");
            e.printStackTrace();
        }
    }

    private int addArtistRecord(Connection conn, String[] fields, String[] fieldNames, FileWriter writer)
            throws SQLException {
        HashMap<String, String> record = new HashMap<String, String>();
        if (this.uniqueRecords.contains(fields[0].replace("\"", "") + "\"")) {
            return -1;
        }
        this.uniqueRecords.add(fields[0].replace("\"", "") + "\"");
        // String fields
        record.put("URI", "\"" + fields[0].replace("\"", "") + "\"");
        record.put("name", "\"" + fields[1] + "\"");
        record.put("comment", "\"" + fields[2] + "\"");
        final int[] stringFields = new int[] { 21, 22, 28, 30, 31, 57, 64, 69, 70, 75, 76, 82, 113,
                114, 116, 125, 128, 129, 137 };
        final int[] yearFields = new int[] { 6, 7, 27, 43 };
        final int[] dateFields = new int[] { 23, 40, 98 };
        final int[] numberFields = new int[] { 54, 75, 115, 132, 133, 136 };
        final String[] orderedColumnNames = { "URI", "name", "comment", "height", "weight", "runtime",
                "activeYearsEndYear", "activeYearsStartYear", "background", "bibsysId", "birthDate", "birthName",
                "birthYear", "bnfId", "bpnId", "deathDate", "deathYear", "individualisedGnd", "isniId", "lccnId",
                "mbaId", "networmbaidth", "nlaId", "numberOfFilms", "orcidId", "recordDate", "restingPlacePosition",
                "ridId", "selibrId", "title", "ulanId", "viafId", "wikiPageId", "wikiPageRevisionId", "description",
                "point", "posLat", "posLong" };
        for (int i : stringFields) {
            if (!fields[i].equals("NULL")) {
                record.put(fieldNames[i], "\"" + fields[i] + "\"");
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
                String dateString = new SimpleDateFormat("\"yyyy-MM-dd\"").format(parseDateFromCsv(fields[i]));
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
        try {
            StringBuilder sb = new StringBuilder();
            for (String i : orderedColumnNames) {
                sb.append(record.getOrDefault(i, ""));
                sb.append(",");
            }
            writer.write(sb.substring(0, sb.length()) + "\n");
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;
    }

    private void addToArtistFKTable(Connection conn, String tableName, String artistUri, String field1) {
        try {
            this.fkTableCSVs.get(tableName).write(String.format("\"%s\", \"%s\"\n", artistUri, field1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addToArtistFKTable(Connection conn, String tableName, String artistUri, String field1, String field2) {
        try {
            this.fkTableCSVs.get(tableName).write(String.format("\"%s\", \"%s\", \"%s\"\n", artistUri, field1, field2));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void generateArtistFKTable1Field(Connection conn, String tableName)
            throws SQLException {
        Statement st;

        st = conn.createStatement();
        try {
            st.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {

        }
        System.out.println("Made table" + tableName);
        st.execute(String.format(
                "CREATE TABLE %s (rId INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT %srpk PRIMARY KEY, " +
                        "artisturi VARCHAR(255), field1 VARCHAR(500))",
                tableName, tableName, tableName));

        File x = new File("csv/" + tableName + "derby.csv");
        if (x.exists()) {
            x.delete();
        }
        try {
            this.fkTableCSVs.put(tableName, new FileWriter(x));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void generateArtistFKTable2Field(Connection conn, String tableName)
            throws SQLException {
        Statement st;

        st = conn.createStatement();
        try {
            st.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {

        }
        System.out.println("made table " + tableName);
        st.execute(String.format(
                "CREATE TABLE %s (rId INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT %srpk PRIMARY KEY, " +
                        "artisturi VARCHAR(255), field1 VARCHAR(255), field2 VARCHAR(500))",
                tableName, tableName, tableName));
        File x = new File("csv/" + tableName + "derby.csv");
        if (x.exists()) {
            x.delete();
        }
        try {
            this.fkTableCSVs.put(tableName, new FileWriter(x));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
