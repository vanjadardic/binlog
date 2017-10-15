package com.bravostudio.binlog;

import com.bravostudio.binlog.event.EventManager;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinLog {

   private static final Logger L = LoggerFactory.getLogger(BinLog.class);
   private static final Pattern ROTATE_LOG_PATTERN = Pattern.compile("^.*\tRotate to (.*)  pos: ([\\d]*)$");
   private final String mysqlbinlog;
   private final String host;
   private final int port;
   private final String user;
   private final String pass;
   private Thread readerThread;
   private Process binLogProcess;
   private boolean running;
   private BinLogPosition binLogPosition;
   private final EventManager eventManager;

   public BinLog(String host, String user, String pass) {
      this("mysqlbinlog", host, 3306, user, pass);
   }

   public BinLog(String mysqlbinlog, String host, int port, String user, String pass) {
      this.mysqlbinlog = mysqlbinlog;
      this.host = host;
      this.port = port;
      this.user = user;
      this.pass = pass;
      readerThread = null;
      binLogProcess = null;
      running = false;
      eventManager = new EventManager();
   }

   private void loadLastBinLogPosition() {
      try (Connection conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/", user, pass);
            ResultSet result = conn.createStatement().executeQuery("SHOW MASTER STATUS");) {
         result.next();
         binLogPosition.setBinLogFileAndPosition(result.getString(1), result.getLong(2));
         L.info("Fetched last binlog position " + binLogPosition);
      } catch (SQLException ex) {
         throw new RuntimeException("Can't determine last binlog position", ex);
      }
   }

   private void terminateBinLogProcess() {
      if (binLogProcess != null) {
         L.info("Terminating binlog process");
         try {
            if (binLogProcess.isAlive()) {
               if (!binLogProcess.waitFor(100, TimeUnit.MILLISECONDS)) {
                  binLogProcess.destroy();
                  if (binLogProcess.isAlive()) {
                     if (!binLogProcess.waitFor(1000, TimeUnit.MILLISECONDS)) {
                        binLogProcess.destroyForcibly();
                        if (binLogProcess.isAlive()) {
                           if (!binLogProcess.waitFor(2000, TimeUnit.MILLISECONDS)) {
                              L.error("Can't terminate binlog process");
                              return;
                           }
                        }
                     }
                  }
               }
            }
            binLogProcess = null;
            L.info("Binlog process terminated successfully");
         } catch (InterruptedException ex) {
            L.error("Interrupted while terminating binlog process", ex);
            binLogProcess.destroyForcibly();
            binLogProcess = null;
         }
      }
   }

   public void start(BinLogPosition binLogPosition) {
      if (!running) {
         this.binLogPosition = binLogPosition;
         L.info("Starting binlog reading at " + binLogPosition);
         if (binLogPosition.getBinlogFile() == null) {
            loadLastBinLogPosition();
         }
         readerThread = new Thread(() -> {
            boolean firstRun = true;
            boolean usingStartPosition = true;
            while (running) {
               if (!firstRun) {
                  try {
                     L.warn("Waiting 1s before starting new binlog process");
                     Thread.sleep(1000);
                  } catch (InterruptedException ex) {
                     if (!running) {
                        break;
                     }
                  }
               }
               firstRun = false;
               try {
                  ProcessBuilder binLogProcessBuilder = prepareBinLogProcess(usingStartPosition);
                  binLogProcess = binLogProcessBuilder.start();
                  L.info("Binlog process started (" + binLogProcessBuilder.command().stream().collect(Collectors.joining(" ")) + ")");
                  BinLogInputStream in = new BinLogInputStream(binLogProcess.getInputStream());
                  byte[] line;
                  while ((line = in.readLine()) != null) {
                     String strLine = new String(line);
                     eventManager.handleLine(strLine, line);
                     boolean binLogFileChanged = updateBinLogPosition(strLine);
                     if (binLogFileChanged && usingStartPosition) {
                        usingStartPosition = false;
                        firstRun = true;
                        L.info("Terminating binlog process for first log rotation");
                        terminateBinLogProcess();
                        break;
                     }
                  }
               } catch (IOException ex) {
                  if (running && !firstRun) {
                     L.error("Error while listening for binlog changes", ex);
                  }
               } finally {
                  if (running && !firstRun) {
                     L.warn("Error while listening for binlog changes");
                     terminateBinLogProcess();
                  }
               }
            }
            L.info("Stopped binlog reading at " + binLogPosition);
         });
         readerThread.setDaemon(true);
         readerThread.start();
         running = true;
      }
   }

   private ProcessBuilder prepareBinLogProcess(boolean usingStartPosition) {
      ProcessBuilder binLogProcessBuilder = new ProcessBuilder(mysqlbinlog,
            "--read-from-remote-server", "--host=" + host, "--user=" + user,
            "--base64-output=decode-rows", "--verbose", "--stop-never",
            binLogPosition.getBinlogFile()
      );
      if (usingStartPosition) {
         binLogProcessBuilder.command().add("--start-position=" + binLogPosition.getPosition());
      }
      binLogProcessBuilder.environment().put("MYSQL_PWD", pass);
      return binLogProcessBuilder;
   }

   private boolean updateBinLogPosition(String line) {
      if (line.startsWith("# at ")) {
         long newPosition = Long.parseLong(line.substring("# at ".length()));
         if (newPosition > binLogPosition.getPosition()) {
            binLogPosition.setPosition(newPosition);
         }
      } else if (line.contains("\tRotate to") && line.contains("  pos:")) {
         Matcher rotateLogMatcher = ROTATE_LOG_PATTERN.matcher(line);
         if (rotateLogMatcher.matches()) {
            String newBinLogFile = rotateLogMatcher.group(1);
            long newPosition = Long.parseLong(rotateLogMatcher.group(2));
            if (!binLogPosition.getBinlogFile().equals(newBinLogFile)) {
               binLogPosition.setBinLogFileAndPosition(newBinLogFile, newPosition);
               return true;
            }
         }
      }
      return false;
   }

   public void stop() {
      if (running) {
         running = false;
         L.info("Stopping binlog reading");
         readerThread.interrupt();
         readerThread = null;
         terminateBinLogProcess();
      }
   }

   public void stop(long millis) throws InterruptedException {
      if (running) {
         running = false;
         L.info("Stopping binlog reading");
         readerThread.interrupt();
         terminateBinLogProcess();
         readerThread.join(millis);
      }
   }

   public EventManager getEventManager() {
      return eventManager;
   }
}
