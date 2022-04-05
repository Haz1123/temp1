import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

class birthDate {

    Date startDate;
    Date endDate;
    DateFormat df = new SimpleDateFormat("yyyyMMdd");
    static birthDate self = new birthDate();

    int pageSize = 0;

    /**
     * @param args
     */
    public static void main(String[] args) {
        long startTime = System.nanoTime();
        self.readComandLineArgs(args);
        if (!self.verifyCommandLineArgs()) {
            System.exit(0);
        }
        File folder = new File("./");
        File[] listOfFiles = folder.listFiles();
        File heapFile = null;
        for (File file : listOfFiles) { // Finds a heap file in the active directory and uses it.
            if (file.getName().matches("heap.\\d+")) {
                heapFile = file;
            }
        }

        Vector<ArtistRecord> output = self.getFilteredRecordsFromHeapfile(heapFile);
        long endTime = System.nanoTime();
        System.out.println(String.format("Query Completed, found %s records in %s milliseconds",
                output.size(), (endTime - startTime) / 1000000));

        if (output.size() > 0) {
            System.out.println(ArtistRecord.TO_STRING_HEADERS);
        }
        for (ArtistRecord record : output) {
            System.out.println(record.toString());
        }
    }

    private Vector<ArtistRecord> getFilteredRecordsFromHeapfile(File heapFile) {
        Vector<ArtistRecord> output = new Vector<ArtistRecord>();
        try {
            self.pageSize = Integer.parseInt(heapFile.getName().substring(heapFile.getName().lastIndexOf(".") + 1));
            FileInputStream inputStream = new FileInputStream(heapFile);
            byte[] nextPage = new byte[self.pageSize];
            // Read pages until none are left
            while (inputStream.read(nextPage) != -1) {
                ArtistRecord[] pageRecords = self.getRecordsFromPage(nextPage);
                for (ArtistRecord artistRecord : pageRecords) {
                    if (dateInclusiveBetween(artistRecord.birthDate, self.startDate, self.endDate)) {
                        output.add(artistRecord);
                    }
                }
            }
            inputStream.close();
        } catch (NullPointerException e) {
            System.err.println("Null pointer exception, probably no heap file found in active directory.");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Issue reading header file's page length.");
            e.printStackTrace();
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println("Header file not found.");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("IO Exception");
            e.printStackTrace();
            e.printStackTrace();
            System.exit(1);
        }
        return output;
    }

    /**
     * Reads and returns records from a byte array or 'page'
     * 
     * @param page
     *            byte array to read records from, including header at end
     * @return ArtistRecord[]
     */
    private ArtistRecord[] getRecordsFromPage(byte[] page) {
        int numRecords = Util.bytesToInt(Arrays.copyOfRange(page, page.length - 8, page.length - 4));
        ArtistRecord[] output = new ArtistRecord[numRecords];
        int[] pageHeaderReverse = Util.bytesToIntArray(
                Arrays.copyOfRange(page, page.length - (4 * (numRecords + 2)), page.length));
        int[] pageHeader = new int[numRecords + 2];
        for (int i = 0; i < pageHeaderReverse.length; i++) {
            pageHeader[pageHeader.length - i - 1] = pageHeaderReverse[i];
        }
        for (int i = 2; i < pageHeader.length - 1; i++) {
            output[i - 2] = new ArtistRecord(Arrays.copyOfRange(page, pageHeader[i], pageHeader[i + 1]));
        }
        // Get last record using the pointer to the start of free space as the end of
        // the record.
        output[output.length - 1] = new ArtistRecord(
                Arrays.copyOfRange(page, pageHeader[pageHeader.length - 1], pageHeader[0]));
        return output;
    }

    /**
     * @param date
     *            date to check
     * @param startDate
     * @param endDate
     * @return boolean True if date between startDate and endDate (inclusive)
     */
    static private boolean dateInclusiveBetween(Date date, Date startDate, Date endDate) {
        if (date == null) {
            return false;
        } else {
            return (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0);
        }
    }

    /**
     * @return boolean True if command line arguments are set, false if not
     */
    private boolean verifyCommandLineArgs() {
        boolean output = true;
        if (startDate == null) {
            output = false;
            System.err.println("Start date can't be null.");
        }
        if (endDate == null) {
            output = false;
            System.err.println("End date can't be null.");
        }
        return output;
    }

    /**
     * @param args
     *            command line arguments
     */
    private void readComandLineArgs(String[] args) {
        try {
            startDate = df.parse(args[0]);
            endDate = df.parse(args[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Not enough arguments, expected a start and end date.");
        } catch (ParseException e) {
            System.err.println("Issue parsing date, check format.");
        }
    }
}