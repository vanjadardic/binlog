package com.bravostudio.binlog.event;

import com.bravostudio.binlog.event.parse.EventParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventManager {

   private static final Logger L = LoggerFactory.getLogger(EventManager.class);
   private EventParser eventParser;
   private final List<EventListener<InsertEvent>> insertEventListeners;
   private final List<EventListener<DeleteEvent>> deleteEventListeners;
   private final List<EventListener<UpdateEvent>> updateEventListeners;

   public EventManager() {
      eventParser = null;
      insertEventListeners = new ArrayList<>();
      deleteEventListeners = new ArrayList<>();
      updateEventListeners = new ArrayList<>();
   }

   public void handleLine(String strLine, byte[] line) {
      if (strLine.startsWith("###")) {
         if (eventParser == null) {
            eventParser = EventParser.generateFromFirstLine(strLine);
         } else {
            eventParser.parseLine(strLine, line);
         }
      } else if (eventParser != null) {
         dispatchEvent(eventParser.generateEvent());
         eventParser = null;
      }
   }

   private void dispatchEvent(Object event) {
      if (event instanceof InsertEvent) {
         dispatchInsertEvent((InsertEvent) event);
      } else if (event instanceof DeleteEvent) {
         dispatchDeleteEvent((DeleteEvent) event);
      } else if (event instanceof UpdateEvent) {
         dispatchUpdateEvent((UpdateEvent) event);
      }
   }

   public void addInsertListener(EventListener<InsertEvent> listener) {
      insertEventListeners.add(listener);
   }

   public void removeInsertListener(EventListener<InsertEvent> listener) {
      insertEventListeners.remove(listener);
   }

   private void dispatchInsertEvent(InsertEvent event) {
      insertEventListeners.forEach(listener -> {
         try {
            if ((listener.getDatabases() == null || listener.getDatabases().length == 0
                  || Arrays.stream(listener.getDatabases()).anyMatch(d -> d.equals(event.getDatabase())))
                  && (listener.getTables() == null || listener.getTables().length == 0
                  || Arrays.stream(listener.getTables()).anyMatch(d -> d.equals(event.getTable())))) {
               event.setCharset(listener.getCharset());
               listener.actionPerformed(event);
            }
         } catch (Exception ex) {
            L.error("Error while dispatching insert event", ex);
         }
      });
   }

   public void addDeleteListener(EventListener<DeleteEvent> listener) {
      deleteEventListeners.add(listener);
   }

   public void removeDeleteListener(EventListener<DeleteEvent> listener) {
      deleteEventListeners.remove(listener);
   }

   private void dispatchDeleteEvent(DeleteEvent event) {
      deleteEventListeners.forEach(listener -> {
         try {
            if ((listener.getDatabases() == null || listener.getDatabases().length == 0
                  || Arrays.stream(listener.getDatabases()).anyMatch(d -> d.equals(event.getDatabase())))
                  && (listener.getTables() == null || listener.getTables().length == 0
                  || Arrays.stream(listener.getTables()).anyMatch(d -> d.equals(event.getTable())))) {
               event.setCharset(listener.getCharset());
               listener.actionPerformed(event);
            }
         } catch (Exception ex) {
            L.error("Error while dispatching delete event", ex);
         }
      });
   }

   public void addUpdateListener(EventListener<UpdateEvent> listener) {
      updateEventListeners.add(listener);
   }

   public void removeUpdateListener(EventListener<UpdateEvent> listener) {
      updateEventListeners.remove(listener);
   }

   private void dispatchUpdateEvent(UpdateEvent event) {
      updateEventListeners.forEach(listener -> {
         try {
            if ((listener.getDatabases() == null || listener.getDatabases().length == 0
                  || Arrays.stream(listener.getDatabases()).anyMatch(d -> d.equals(event.getDatabase())))
                  && (listener.getTables() == null || listener.getTables().length == 0
                  || Arrays.stream(listener.getTables()).anyMatch(d -> d.equals(event.getTable())))) {
               event.setCharset(listener.getCharset());
               listener.actionPerformed(event);
            }
         } catch (Exception ex) {
            L.error("Error while dispatching update event", ex);
         }
      });
   }
}
