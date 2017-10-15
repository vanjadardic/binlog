package com.bravostudio.binlog.event.parse;

import com.bravostudio.binlog.event.InsertEvent;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

class InsertEventParser extends EventParser<InsertEvent> {

   private final String database;
   private final String table;
   private final List<byte[]> fields;

   InsertEventParser(String database, String table) {
      this.database = database;
      this.table = table;
      fields = new ArrayList<>();
   }

   @Override
   public void parseLine(String strLine, byte[] line) {
      if ("### SET".equals(strLine)) {
         return;
      }
      Matcher valueMatcher = VALUE_PATTERN.matcher(strLine);
      if (valueMatcher.matches()) {
         fields.add(convertValue(line, valueMatcher.start(1)));
      }
   }

   @Override
   public InsertEvent generateEvent() {
      return new InsertEvent() {
         private List<String> stringFields;
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
         public void setCharset(Charset charset) {
            if (this.charset == null && charset != null || this.charset != null && !this.charset.equals(charset)) {
               stringFields = null;
            }
            this.charset = charset;
         }
      };
   }
}
