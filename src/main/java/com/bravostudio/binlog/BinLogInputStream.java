package com.bravostudio.binlog;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class BinLogInputStream extends InputStream implements Closeable {

   private final InputStream in;
   private final ByteArrayOutputStream buf;

   public BinLogInputStream(InputStream in) {
      this.in = in;
      buf = new ByteArrayOutputStream();
   }

   @Override
   public int read() throws IOException {
      return in.read();
   }

   public byte[] readLine() throws IOException {
      buf.reset();
      int b;
      while ((b = read()) != -1) {
         if (b == '\n') {
            break;
         } else if (b != '\r') {
            buf.write(b);
         }
      }
      return buf.toByteArray();
   }
}
