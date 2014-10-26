
import java.io.IOException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class FileInputFormatB extends FileInputFormat<LongWritable,Text>{
	 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new CustomLineRecordReader();
    }
}

class CustomLineRecordReader extends RecordReader<LongWritable, Text> {
	 
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private NewLineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    

    // ----------------------
    // set the delimiter of two adjacent records
	
	//Character ttsep=6;
	//Delimiter_Title_Text=ttsep.toString();
	String Delimiter_Page_Page="<eof>";
	//Character ppsep=5;
	//Delimiter_Page_Page=ppsep.toString();
    
    private byte[] separator = Delimiter_Page_Page.getBytes();
 
    // --------------------
 
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);
 
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new NewLineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                this.start -= separator.length;//
                // --start;
                fileIn.seek(start);
            }
            in = new NewLineReader(fileIn, job);
        }
        if (skipFirstLine) { // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
       this.pos = start;
    }
 
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();

        }
       int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
 
        }
       if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
 
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }
 
   @Override
    public Text getCurrentValue() {

      return value;
    }
 
    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
 
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
 
    public class NewLineReader {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private InputStream in;
        private byte[] buffer;
        private int bufferLength = 0;
        private int bufferPosn = 0;
 
        public NewLineReader(InputStream in) {
            this(in, DEFAULT_BUFFER_SIZE);
        }
 
        public NewLineReader(InputStream in, int bufferSize) {
            this.in = in;
            this.bufferSize = bufferSize;
            this.buffer = new byte[this.bufferSize];
        }
 
        public NewLineReader(InputStream in, Configuration conf)
                throws IOException {
            this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
        }
 
        public void close() throws IOException {
            in.close();
        }
 
        public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
                throws IOException {
            str.clear();
            Text record = new Text();
            int txtLength = 0;
            long bytesConsumed = 0L;
            boolean newline = false;
            int sepPosn = 0;
            do {
                
                // reach the end of buffer and reader the next buffer

                if (this.bufferPosn >= this.bufferLength) {
                    bufferPosn = 0;
                    bufferLength = in.read(buffer);


                    // reach the end of a file and break; or, read the next file
                    if (bufferLength <= 0) {
                        break;
                    }
                }
                int startPosn = this.bufferPosn;
                for (; bufferPosn < bufferLength; bufferPosn++) {
                    
                    if (sepPosn > 0 && buffer[bufferPosn] != separator[sepPosn]) {
                        sepPosn = 0;
                    }
                    // the first letter of the delimiter
                    if (buffer[bufferPosn] == separator[sepPosn]) {
                        bufferPosn++;
                        int i = 0;
                        
                        for (++sepPosn; sepPosn < separator.length; i++, sepPosn++) {
                            
                            if (bufferPosn + i >= bufferLength) {
                                bufferPosn += i - 1;
                                break;
                            }
                            
                            if (this.buffer[this.bufferPosn + i] != separator[sepPosn]) {
                                sepPosn = 0;
                                break;
                            }
                        }
                        
                        if (sepPosn == separator.length) {
                            bufferPosn += i;
                            newline = true;
                            sepPosn = 0;
                            break;
                        }
                    }
                }
                int readLength = this.bufferPosn - startPosn;
                bytesConsumed += readLength;
                
                if (readLength > maxLineLength - txtLength) {
                    readLength = maxLineLength - txtLength;
                }
                if (readLength > 0) {
                    record.append(this.buffer, startPosn, readLength);
                    txtLength += readLength;
                    
                    if (newline) {
                        str.set(record.getBytes(), 0, record.getLength()
                                - separator.length);
                    }
                }
            } while (!newline && (bytesConsumed < maxBytesToConsume));
            if (bytesConsumed > (long) Integer.MAX_VALUE) {
                throw new IOException("Too many bytes before newline: "
                        + bytesConsumed);
            }
 
            return (int) bytesConsumed;
        }
 
        public int readLine(Text str, int maxLineLength) throws IOException {
            return readLine(str, maxLineLength, Integer.MAX_VALUE);
        }
 
        public int readLine(Text str) throws IOException {
            return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
    }
}

