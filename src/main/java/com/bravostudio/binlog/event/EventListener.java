package com.bravostudio.binlog.event;

import java.nio.charset.Charset;

public interface EventListener<T> {

   void actionPerformed(T event);

   default String[] getDatabases() {
      return null;
   }

   default String[] getTables() {
      return null;
   }

   default Charset getCharset() {
      return null;
   }
}
