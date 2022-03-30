import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class dbload {

    String inputDataFile = "";
    Integer pageSize = -1;

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) throws Exception {
        dbload dbload = new dbload();
        // Read command line arguments
        dbload.readCommandLineArgs(args);
        if (!dbload.verifyCommandLineArgs()) {
            System.exit(0);
        }
        // Setup io files
        File outputFile = new File("heap." + dbload.pageSize.toString());
        File artistsFile = new File(dbload.inputDataFile);

        // Open input file
        FileReader fileReader = new FileReader(artistsFile);
        BufferedReader inputFileBuffer = new BufferedReader(fileReader);

        // Open output file
        if (outputFile.exists()) {
            outputFile.delete();
        }
        FileOutputStream outputStream = new FileOutputStream(outputFile, true);

        byte[] pageToWrite = new byte[dbload.pageSize];
        int[] pageHeader = new int[100];
        int pageCount = 0;
        int currentPageSize = 0;
        int pageHeaderSize = 2;
        Arrays.fill(pageToWrite, (byte) 0);

        int numRecords = 0;

        String readString = "";
        // Remove first 4 lines from buffer
        for (int i = 0; i < 5; i++) {
            readString = inputFileBuffer.readLine();
        }
        while (readString != null) {

            ArtistRecord record = new ArtistRecord(readString);
            byte[] recordBytes = record.getRecordAsBytes();
            numRecords++;

            // Make new page if current record won't fit on current page.
            if (currentPageSize + ((pageHeaderSize + 1) * 4) + recordBytes.length > dbload.pageSize) {
                addHeaderToPage(pageToWrite, pageHeader, currentPageSize, pageHeaderSize);
                // Write page to file
                dbload.writePage(outputStream, pageToWrite, pageCount);
                pageCount++;
                // Reset temp and per page variables
                currentPageSize = 0;
                pageHeaderSize = 2;
                Arrays.fill(pageToWrite, (byte) 0);
                Arrays.fill(pageHeader, 0); // Not strictly needed but helps debugging
            }

            // Add record to current page
            Util.arrayMerge(recordBytes, pageToWrite, recordBytes.length, 0, currentPageSize);
            // Add pointers to header
            pageHeader[pageHeaderSize] = currentPageSize;
            pageHeaderSize++;
            currentPageSize += recordBytes.length;
            readString = inputFileBuffer.readLine();
        }

        // Write last page
        addHeaderToPage(pageToWrite, pageHeader, currentPageSize, pageHeaderSize);
        dbload.writePage(outputStream, pageToWrite, pageCount);

        inputFileBuffer.close();

        System.out.println(String.format("Wrote %s records to %s pages", numRecords, pageCount));
    }

    private static void addHeaderToPage(byte[] page, int[] pageHeader, int currentPageSize, int pageHeaderSize) {
        pageHeader[0] = currentPageSize;
        pageHeader[1] = pageHeaderSize - 2;
        // Add header to page
        byte[] pageHeaderBytes = new byte[pageHeaderSize * 4];
        for (int i = 0; i < pageHeaderSize; i++) {
            Util.arrayMerge(Util.intToBytes(pageHeader[pageHeaderSize - 1 - i]), pageHeaderBytes, 4, 0, i * 4);
        }
        Util.arrayMerge(pageHeaderBytes, page, pageHeaderBytes.length, 0,
                page.length - pageHeaderBytes.length);
    }

    /**
     * Sets dbload variables as needed
     * 
     * @param args
     *            Arguments to read
     */
    private void readCommandLineArgs(String[] args) {
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case ("-p"):
                    case ("--pagesize"):
                        this.pageSize = Integer.valueOf(args[i + 1]);
                        i++;
                    default:
                        this.inputDataFile = args[i];

                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Expected an argument after a flag and didn't recieve it.");
        }
    }

    /**
     * Checks if the required dbload variables are set correctly, does not check if
     * files exist.
     * 
     * @return boolean true if arguments are set correctly, false if not.
     */
    private boolean verifyCommandLineArgs() {
        if (this.inputDataFile == "") {
            System.err.println("Input data file not specified");
            return false;
        } else if (this.pageSize == -1) {
            System.err.println("Pagesize not specified.");
            return false;
        }
        return true;
    }

    /**
     * Writes a 'page' to outputFile.
     * 
     * @param outputFile
     *            File to write to.
     * @param bytes
     *            Bytes to write.
     */
    private boolean writePage(FileOutputStream outputFile, byte[] bytes, int pageNum) {
        boolean success = true;
        try {
            outputFile.write(bytes);
            outputFile.flush();
        } catch (IOException e) {
            System.err.println("Issue writing to output file.");
            success = false;
        }
        return success;
    }

}
