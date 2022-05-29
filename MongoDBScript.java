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

public class MongoDBScript {
    public String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    public String protocol = "jdbc:derby:";
    private File artistMongoCSV = new File("mongo_artist.csv");
    private FileWriter artistsFileWriter;
    private HashMap<String, FileWriter> fkTableCSVs;
    private HashSet<String> uniqueRecords;

    public static void main(String[] args) throws Exception {
        MongoDBScript dt = new MongoDBScript();
        dt.makeCSVs();
        dt.loadCSVs();
        // Post impressionists -
    }

    private void loadCSVs() throws SQLException {

    }

    public void makeCSVs() throws Exception {
        // File setups
        System.out.println("Building Tables");
        File artistsFile = new File("artist.csv");
        FileReader fileReader = new FileReader(artistsFile);
        BufferedReader inputFileBuffer = new BufferedReader(fileReader);
        this.artistsFileWriter = new FileWriter(artistMongoCSV);
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
            addArtistRecord(fields, colNames, artistsFileWriter);
            readString = inputFileBuffer.readLine();
        }
        this.artistsFileWriter.close();
        inputFileBuffer.close();

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

    private void addArtistRecord(String[] fields, String[] fieldNames, FileWriter writer)
            throws SQLException, IOException {

        if (this.uniqueRecords.contains(fields[0].replace("\"", "") + "\"")) {
            return;
        }
        this.uniqueRecords.add(fields[0].replace("\"", "") + "\"");

        RecordJSONBuilder record = new RecordJSONBuilder();

        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].equals("NULL")) {
                switch (i) {
                    case (0):
                        record.put("URI", fields[i].replace("\"", ""));
                        break;
                    case (1):
                        record.name.put("current", fields[i]);
                        break;
                    case (2):
                        record.wiki.put("comment", fields[i]);
                        break;
                    case (5): // runtime
                        record.put(fieldNames[i], fields[i]);
                        break;
                    case (6): // activeyearend
                        record.activeYears.put("endYear", fields[i]);
                        break;
                    case (7): // ActiveYearstart
                        record.activeYears.put("startYear", fields[i]);
                        break;
                    case (8): // alias
                    case (24): // birthName
                    case (97):// psyeudonym
                        record.name.put(fieldNames[i], fields[i].replace("{", "").replace("}", "").split("\\|"));
                        break;
                    case (10): // almaMater
                    case (45): // education
                    case (127):// training
                        record.education.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (12): // artist
                        record.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (14): // associated acts
                    case (16): // assocated bands
                    case (18): // assocated musical artist
                    case (20): // award
                    case (30): // board
                    case (47): // employer
                    case (51):// field
                    case (53):// genre
                    case (59):// influenced
                    case (63):// instrument
                    case (66):// knownFor
                    case (72):// movement
                    case (78):// noteable work
                    case (81):// occupation
                    case (90):// personfunction
                    case (96):// producer
                    case (100):// record label
                    case (102):// recordedIn
                    case (122):// subsequent work
                        record.work.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (21): // background
                        record.background.put(fieldNames[i], fields[i]);
                        break;
                    case (23): // birthDate
                    case (40):// deathDate
                        record.biography.put(fieldNames[i], parseDateFromCsv(fields[i]));
                        break;
                    case (26): // Birthplace
                    case (35): // citizenship
                    case (37): // country
                    case (39): // deathCause
                    case (44): // deathPlace
                    case (49): // ethnicity
                    case (56): // hometown
                    case (70): // language
                    case (74): // nationality
                    case (88): // party
                    case (108):// religion
                    case (110):// residence
                    case (112):// restingPlace
                    case (113):// restingPlacePosition
                    case (120):// state of origin
                        record.biography.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (27): // birthYear
                    case (43): // deathYear
                        record.biography.put(fieldNames[i],
                                Integer.parseInt(new SimpleDateFormat("yyyy").format(parseDateFromCsv(fields[i]))));
                        break;
                    case (33): // child
                    case (84): // parent
                    case (86): // partner
                    case (104): // relation
                    case (106): // relative
                    case (118): // spouse
                        record.relationship.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (54):// height
                    case (75):// net worth
                    case (132):// weight
                        record.biography.put(fieldNames[i], fields[i]);
                        break;
                    case (61):// influenced by
                        record.background.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (79):// number of films
                        record.work.put(fieldNames[i], fields[i]);
                        break;
                    case (92): // predecessor
                    case (94): // previous work
                    case (131):// voiceType
                        record.background.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (98):
                        record.work.put(fieldNames[i], parseDateFromCsv(fields[i]));
                        break;
                    case (124): // thumbNail
                        record.wiki.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    case (125): // title
                        record.name.put(fieldNames[i], fields[i]);
                        break;
                    case (133): // wikiPageID
                    case (136): // wiki page revision id
                    case (137):// description
                        record.wiki.put(fieldNames[i], fields[i]);
                        break;
                    case (135): // wikiPageRedirects
                        record.wiki.put(fieldNames[i], getLabelURIPairs(fields[i - 1], fields[i]));
                        break;
                    // TODO ADD IDS
                }
            }
        }
        writer.write(record.getStringRep() + "\n");

    }

    private HashSet<HashMap<String, String>> getLabelURIPairs(String labelString, String uriString) {
        String[] labels = labelString.replace("{", "").replace("}", "").split("\\|");
        String[] uris = uriString.replace("{", "").replace("}", "").split("\\|");
        HashSet<HashMap<String, String>> data = new HashSet<>();
        for (int j = 0; j < uris.length; j++) {
            HashMap<String, String> x = new HashMap<String, String>();
            try {
                x.put("label", labels[j]);
            } catch (IndexOutOfBoundsException e) {
            }
            try {
                x.put("uri", uris[j]);
            } catch (IndexOutOfBoundsException e) {
            }
            data.add(x);
        }
        return data;
    }

    private class RecordJSONBuilder extends HashMap<String, Object> {

        public HashMap<String, Object> biography = new HashMap<String, Object>();
        public HashMap<String, Object> education = new HashMap<String, Object>();
        public HashMap<String, Object> relationship = new HashMap<String, Object>();
        public HashMap<String, Object> name = new HashMap<String, Object>();
        public HashMap<String, Object> wiki = new HashMap<String, Object>();
        public HashMap<String, Object> work = new HashMap<String, Object>();
        public HashMap<String, Object> background = new HashMap<String, Object>();
        public HashMap<String, Object> activeYears = new HashMap<String, Object>();

        public String getStringRep() {
            biography.put("education", education);
            biography.put("relationship", relationship);
            biography.put("name", name);
            this.put("biography", biography);
            this.put("wiki", wiki);
            this.work.put("background", background);
            this.work.put("activeYears", activeYears);
            this.put("work", work);
            String x = this.HashMapToString(this);
            return x;

        }

        private String HashMapToString(HashMap<String, ?> hm) {
            StringBuilder sb = new StringBuilder();
            if (this.keySet().size() == 0) {
                return "";
            }
            sb.append("{");
            for (String key : hm.keySet()) {
                Object val = hm.get(key);
                if (val instanceof HashMap<?, ?>) {
                    sb.append("\"" + key + "\"" + ":" + HashMapToString((HashMap<String, Object>) val));
                } else if (val instanceof String) {
                    sb.append("\"" + key + "\"" + ":\"" + val + "\"");
                } else if (val instanceof HashSet<?>) {
                    sb.append("\"" + key + "\"" + ":" + "[");
                    for (Object element : (HashSet<?>) val) {
                        sb.append(HashMapToString((HashMap<String, String>) element));
                        sb.append(",");
                    }
                    sb.deleteCharAt(sb.lastIndexOf(","));
                    sb.append("]");
                } else if (val instanceof String[]) {
                    sb.append("\"" + key + "\"" + ":" + "[\"");
                    sb.append(String.join("\",\"", (String[]) val));
                    sb.append("\"]");
                } else if (val instanceof Date) {
                    sb.append("\"" + key + "\":\"" + new SimpleDateFormat("yyyy-MM-dd").format((Date) val) + "\"");
                } else if (val instanceof Integer) {
                    sb.append("\"" + key + "\":" + val.toString());
                } else if (val == null) {
                    if (sb.lastIndexOf(",") > 1) {
                        sb.deleteCharAt(sb.lastIndexOf(","));
                    }
                } else {
                    System.out.println(key + " " + val.getClass().getCanonicalName());
                    if (sb.lastIndexOf(",") > 1) {
                        sb.deleteCharAt(sb.lastIndexOf(","));
                    }
                }
                sb.append(",");
            }
            if (sb.lastIndexOf(",") > 1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            sb.append("}");
            return sb.toString().replaceAll("\\\\", "").replace('\t', ' ');
        }

    }
}
