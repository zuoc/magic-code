import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by zuoc on 2017/11/4.
 */
public final class FallbackCallback implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(FallbackCallback.class);

    private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("Kafka-Producer-Fallback-%d").build());

    private static BackupFile zero;

    private static List<BackupFile> backupFiles;

    static {
        if (BackupFile.exists0("/tmp/kafka")) {
            zero = BackupFile.load0("/tmp/kafka");
        } else {
            zero = BackupFile.new0("/tmp/kafka");
        }
    }

    private final ProducerRecord record;

    public FallbackCallback(ProducerRecord record) {
        this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
    }

    private static final class BackupFile {

        private static final byte[] BACKPU_MAGIC = new byte[]{'#', 'B', 'A', 'C', 'K', 'U', 'P', '#'};

        private static final int CREATE_TIMESTAMP_SIZE = 8;

        private static final byte[] RECORD_MAGIC = new byte[]{'-', '_', '-', '!'};

        private static final int RECORD_LENGTH_SIZE = 4;

        private static final String CURRENT_FILE_NAME = "0";

        private static final String BACKUP_FILE_NAME_SUFFIX = ".backup";

        private static final int OS_PAGE_SIZE = 1024 * 4;

        private static final int FILE_SIZE = OS_PAGE_SIZE;

        private File file;

        private MappedByteBuffer mmap;

        private BackupFile(Path path) throws IOException {
            file = path.toFile();
            mmap = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
        }

        private void init() {
            mmap.put(BACKPU_MAGIC);
            mmap.putLong(Instant.now().toEpochMilli());
        }

        private void restore() {
            final byte[] backupMagic = new byte[BACKPU_MAGIC.length];
            mmap.get(backupMagic);
            if (!Arrays.equals(backupMagic, BACKPU_MAGIC)) {
                throw new IllegalStateException("文件 [" + file.getPath() + "] 不是合法的备份文件");
            }
            final LocalDateTime createTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(mmap.getLong()), ZoneId.systemDefault());

            final ByteBuffer byteBuffer = mmap.slice();
            final byte[] recordMagic = new byte[RECORD_MAGIC.length];

            byteBuffer.mark();
            byteBuffer.get(recordMagic);
            while (Arrays.equals(recordMagic, RECORD_MAGIC)) {
                final int recordLength = byteBuffer.getInt();
                byteBuffer.position(byteBuffer.position() + recordLength);
                byteBuffer.mark();
                byteBuffer.get(recordMagic);
            }
            byteBuffer.reset();

            mmap.position(mmap.position() + byteBuffer.position());

            if (logger.isInfoEnabled()) {
                logger.info("成功加载 [{}], 创建时间为: {}", file.getPath(), createTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm-ss")));
            }
        }

        private void append0(byte[] data) {
            if (mmap.remaining() < data.length + RECORD_MAGIC.length + RECORD_LENGTH_SIZE) {
                mmap.force();
                return;
            }
            mmap.put(RECORD_MAGIC);
            mmap.putInt(data.length);
            mmap.put(data);
        }

        private static boolean exists0(String dir) {
            return Files.exists(Paths.get(dir, CURRENT_FILE_NAME));
        }

        private static BackupFile load0(String dir) {
            try {
                final Path path = Paths.get(dir, CURRENT_FILE_NAME);

                final BackupFile backupFile = new BackupFile(path);
                backupFile.restore();
                return backupFile;

            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private static BackupFile new0(String dir) {
            try {
                final Path path = Paths.get(dir, CURRENT_FILE_NAME);
                if (Files.notExists(path.getParent())) {
                    Files.createDirectories(path.getParent());
                }
                Files.createFile(path);

                final BackupFile backupFile = new BackupFile(path);
                backupFile.init();
                return backupFile;

            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

    }

    public static void main(String[] args) {
        System.out.println(FallbackCallback.zero.mmap);
    }

}
