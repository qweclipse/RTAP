import java.nio.file.Files;
import java.nio.file.Path;

// Windows lacks a reliable SIGTERM for JVMs; this invokes Kafka's shutdown hook.
public class LabKafka {
    public static void main(String[] args) {
        Thread watcher = new Thread(() -> {
            while (!Files.exists(Path.of(args[0]))) {
                try { Thread.sleep(500); }
                catch (InterruptedException e) { return; }
            }
            System.exit(0);
        });
        watcher.setDaemon(true);
        watcher.start();
        kafka.Kafka.main(new String[]{args[1]});
    }
}
