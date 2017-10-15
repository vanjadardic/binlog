package com.bravostudio.binlog.event.parse;

import com.bravostudio.binlog.event.UpdateEvent;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

class UpdateEventParser extends EventParser<UpdateEvent> {

   private final String database;
   private final String table;
   private final List<byte[]> fields;
   private final List<byte[]> oldFields;
   private boolean parsingFields;

   UpdateEventParser(String database, String table) {
      this.database = database;
      this.table = table;
      fields = new ArrayList<>();
      oldFields = new ArrayList<>();
   }

   @Override
   public void parseLine(String strLine, byte[] line) {
      if ("### WHERE".equals(strLine)) {
         parsingFields = false;
      } else if ("### SET".equals(strLine)) {
         parsingFields = true;
      } else {
         Matcher valueMatcher = VALUE_PATTERN.matcher(strLine);
         if (valueMatcher.matches()) {
            if (parsingFields) {
               fields.add(convertValue(line, valueMatcher.start(1)));
            } else {
               oldFields.add(convertValue(line, valueMatcher.start(1)));
            }
         }
      }
   }

   @Override
   public UpdateEvent generateEvent() {
      return new UpdateEvent() {
         private List<String> stringFields;
         private List<String> stringOldFields;
         private Charset charset;

         @Override
         public String getDatabase() {
            return database;
         }

         @Override
         public String getTable() {
            return table;
         }

         @Override
         public List<String> getFields() {
            if (stringFields == null) {
               stringFields = fields.stream()
                     .map(f -> f == null ? null : (charset == null ? new String(f) : new String(f, charset)))
                     .collect(Collectors.toList());
            }
            return stringFields;
         }

         @Override
         public List<byte[]> getFieldsByte() {
            return fields;
         }

         @Override
         public List<String> getOldFields() {
            if (stringOldFields == null) {
               stringOldFields = oldFields.stream()
                     .map(f -> f == null ? null : (charset == null ? new String(f) : new String(f, charset)))
                     .collect(Collectors.toList());
            }
            return stringOldFields;
         }

         @Override
         public List<byte[]> getOldFieldsByte() {
            return oldFields;
         }

         @Override
         public void setCharset(Charset charset) {
            if (this.charset == null && charset != null || this.charset != null && !this.charset.equals(charset)) {
               stringFields = stringOldFields = null;
            }
            this.charset = charset;
         }
      };
   }
}
