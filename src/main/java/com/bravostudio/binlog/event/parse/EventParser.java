package com.bravostudio.binlog.event.parse;

import java.io.ByteArrayOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EventParser<T> {

   private static final Logger L = LoggerFactory.getLogger(EventParser.class);
   static final Pattern INSERT_PATTERN = Pattern.compile("^" + Pattern.quote("### INSERT INTO ") + "`(.*)`\\.`(.*)`$");
   static final Pattern DELETE_PATTERN = Pattern.compile("^" + Pattern.quote("### DELETE FROM ") + "`(.*)`\\.`(.*)`$");
   static final Pattern UPDATE_PATTERN = Pattern.compile("^" + Pattern.quote("### UPDATE ") + "`(.*)`\\.`(.*)`$");
   static final Pattern VALUE_PATTERN = Pattern.compile("^" + Pattern.quote("###   @") + "[\\d]+=(.*)$");

   public static EventParser generateFromFirstLine(String line) {
      if (line.startsWith("### INSERT INTO ")) {
         Matcher insertMatcher = INSERT_PATTERN.matcher(line);
         if (insertMatcher.matches()) {
            return new InsertEventParser(insertMatcher.group(1), insertMatcher.group(2));
         }
      } else if (line.startsWith("### DELETE FROM ")) {
         Matcher deleteMatcher = DELETE_PATTERN.matcher(line);
         if (deleteMatcher.matches()) {
            return new DeleteEventParser(deleteMatcher.group(1), deleteMatcher.group(2));
         }
      } else if (line.startsWith("### UPDATE ")) {
         Matcher updateMatcher = UPDATE_PATTERN.matcher(line);
         if (updateMatcher.matches()) {
            return new UpdateEventParser(updateMatcher.group(1), updateMatcher.group(2));
         }
      }
      return null;
   }

   public abstract void parseLine(String strLine, byte[] line);

   public abstract T generateEvent();

   static int convertHexDigit(int c) {
      if (c >= '0' && c <= '9') {
         return c - '0';
      } else {
         return 10 + c - 'a';
      }
   }

   static byte[] convertStringValue(byte[] value, int offset, int length) {
      ByteArrayOutputStream baos = null;
      int pos = offset;
      while (pos < offset + length) {
         if (pos < offset + length - 3 && (value[pos] & 0xFF) == '\\' && (value[pos + 1] & 0xFF) == 'x') {
            if (baos == null) {
               baos = new ByteArrayOutputStream(length - 3);
               if (pos > offset) {
                  baos.write(value, offset, pos - offset);
               }
            }
            baos.write(convertHexDigit(value[pos + 2] & 0xFF) * 16 + convertHexDigit(value[pos + 3] & 0xFF));
            pos += 4;
         } else {
            if (baos != null) {
               baos.write(value, pos, 1);
            }
            pos++;
         }
      }
      if (baos != null) {
         return baos.toByteArray();
      } else {
         byte[] retVal = new byte[length];
         System.arraycopy(value, offset, retVal, 0, length);
         return retVal;
      }
   }

   static byte[] convertValue(byte[] value, int offset) {
      if (value.length == offset + 4
            && (value[offset] & 0xFF) == 'N'
            && (value[offset + 1] & 0xFF) == 'U'
            && (value[offset + 2] & 0xFF) == 'L'
            && (value[offset + 3] & 0xFF) == 'L') {
         return null;
      } else if ((value[offset] & 0xFF) == '\'' && (value[value.length - 1] & 0xFF) == '\'') {
         return convertStringValue(value, offset + 1, value.length - offset - 2);
      } else {
         byte[] retVal = new byte[value.length - offset];
         System.arraycopy(value, offset, retVal, 0, value.length - offset);
         return retVal;
      }
   }
}
