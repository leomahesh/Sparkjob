package com.rta.ec3;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
 * This class is used to process flume agents logs and monitor the logs for ERROR/FATAL
 * @Creator:Rahul Kr (EC3) on 01-08-2020
 * ==========================================
 * @updated by: Rahul kr(EC3)
 * @updated on: 10-11-2020
 * */
/**
 * Custom interceptor, implement Interceptor interface, and implement its abstract method
 */
public class LogCheckInterceptor implements Interceptor {
    private static final String PARAM = "param";
    private static final String SOURCE_NAME = "_rta_source";

    //Print logs to facilitate the execution order of test methods
    private static final Logger LOGGER = Logger.getLogger(LogCheckInterceptor.class.getName());
    //Custom interceptor parameters, used to receive custom interceptor flume configuration parameters
    private static String param = "";
    private static String scriptAbslPath = "";
    private static String scriptName = "";

    /**
     * The interceptor construction method is called in the build method of the static inner class of the custom interceptor to create a custom interceptor object.
     */
    public LogCheckInterceptor() {
        LOGGER.info("----------Custom interceptor construction method execution");
    }


    /**
     * This method is used to initialize the interceptor, which is executed after the construction method of the interceptor is executed, that is, after the interceptor object is created.
     */
    @Override
    public void initialize() {
        LOGGER.info("---------- custom interceptor initialize method execution");
    }

    /**
     * Used to process every event object, this method will not be automatically called by the system, and is generally called inside the List<Event> intercept(List<Event> events) method.
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        LOGGER.info("----------intercept(Event event) method is executed::param: " + param);
        /*
                 Each event processing each line and check for Error tag then call the respective scripts
         */
        String eventBody = new String(event.getBody());
        LOGGER.info("----------intercept(Event event) method is executed::eventBody: " + eventBody);
        checkCaller(eventBody);
        byte[] modifiedEvent = "sink_null".getBytes();
        event.setBody(modifiedEvent);
        return event;
    }

    /**
     * Used to process a batch of event object collections, the collection size is related to the flume startup configuration, and the size of the transactionCapacity remains the same. Generally call Event intercept(Event event) directly to process each event data.
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {

        /*
                 The processing code for the event object collection is generally written here. It is generally to traverse the event object collection. For each event object, call the Event intercept(Event event) method, and then according to whether the return value is null,
                 To add it to the new collection.
         */
        List<Event> results = new ArrayList<>();
        Event event;
        for (Event e : events) {
            event = intercept(e);
            LOGGER.info("----------return the interceptor::");
            if (event != null) {
                results.add(event);
            }
        }
        return results;
    }

    /**
     * This method is mainly used to destroy the interceptor object value execution, generally some processing to release resources
     */
    @Override
    public void close() {
        LOGGER.info("----------Custom interceptor close method execution");
    }

    /**
     * Create a custom object for use by Flume through this static inner class, implement the Interceptor.Builder interface, and implement its abstract method
     */
    public static class Builder implements Interceptor.Builder {

        /**
         * This method is mainly used to return the created custom class interceptor object
         *
         * @return
         */
        @Override
        public Interceptor build() {
            return new LogCheckInterceptor();
        }

        /**
         * Used to receive flume configuration custom interceptor parameters
         *
         * @param context Through this object, you can get the parameters of the flume configuration custom interceptor
         */
        @Override
        public void configure(Context context) {
            /* Get the parameters of the flume configuration custom interceptor by calling the getString method of the context object.
              The method parameters must be consistent with the parameters in the custom interceptor configuration +
              it will consist absolute script path name & script name
             */
            param = context.getString(PARAM);
            scriptAbslPath = param.split(",")[0];
            scriptName = param.split(",")[1];
        }
    }
  /*this is the caller and it will chk for error in each line in flume logs*/
    private Boolean checkCaller(String content) {
        LOGGER.info("--------------------inside checkCaller() "+content);
        if (content.contains("ERROR") || content.contains("FATAL")) {
            if (content.contains("rta_bus_source")
                    || (content.contains("rta_bus_kafka_sink"))
                    || (content.contains("bus_kafka_fileChannel"))
                    || (content.contains("bus"))
                    || (content.contains("busdelays"))) {
                LOGGER.error("-------------------- occured in busdelays flume agent");
                callCommand("busdelays");
            }
            if (content.contains("rta_metro_source")
                    || (content.contains("rta_metro_kafka_sink"))
                    || (content.contains("metro_kafka_fileChannel"))
                    || (content.contains("metro"))
                    || (content.contains("metrodelays"))) {
                callCommand("metrodelays");
                LOGGER.error("-------------------- occured in metrodelays flume agent");
            }
            if (content.contains("rta_tram_source")
                    || (content.contains("rta_tram_kafka_sink"))
                    || (content.contains("tram_kafka_fileChannel"))
                    || (content.contains("tram"))
                    || (content.contains("tramdelays"))) {
                callCommand("tramdelays");
                LOGGER.error("-------------------- occured in tramdelays flume agent");
            }
            if (content.contains("rta_taxibook_source")
                    || (content.contains("rta_taxibook_kafka_sink"))
                    || (content.contains("taxibook_kafka_fileChannel"))
                    || (content.contains("taxiBook"))
                    || (content.contains("taxibookdelays"))) {
                callCommand("taxibookdelays");
                LOGGER.error("-------------------- occured in taxibookdelays flume agent");
            }
            if (content.contains("rta_taxiloc_source")
                    || (content.contains("rta_taxiloc_kafka_sink"))
                    || (content.contains("taxiloc_kafka_fileChannel"))
                    || (content.contains("taxiLoc"))
                    || (content.contains("taxilocationdelays"))) {
                callCommand("taxilocationdelays");
                LOGGER.error("-------------------- occured in TaxiLocationdelays flume agent");
            }
            if (content.contains("rta_inrix_source") || content
                    .contains("rta_inrix_kafka_sink") || content
                    .contains("kafka_inrix_fileChannel") || content
                    .contains("inrixtraffic") || content
                    .contains("EC3_INRIX_TRAFFIC_ANALYSIS")) {
                LOGGER.error("-------------------- occured in inrixtraffic flume agent");
                callCommand("inrixtraffic");
            }
// address the tcs
            if (content.contains("rta_tcs_source")
                    || (content.contains("rta_tcs_kafka_sink"))
                    || (content.contains("tcs_kafka_fileChannel"))
                    || (content.contains("tcs"))
                    || (content.contains("trafficcountdata"))
                    ||(content.contains("EC3_TRAFFIC_COUNT_DATA_ANALYSIS"))) {
                callCommand("trafficcountdata");
                LOGGER.error("-------------------- occured in TCS Traffic Count");
            }
            //address the D8
            if (content.contains("rta_d8_source")
                    || (content.contains("rta_d8_kafka_sink"))
                    || (content.contains("d8_kafka_taxiloc_fileChannel"))
                    || (content.contains("EC3_D8_TAXI_LOCATION_OCC_ANALYSIS"))
                    || (content.contains("taxilocationd8"))) {
                callCommand("taxilocationd8");
                LOGGER.error("-------------------- occured in D8_TaxiLocationdelays flume agent");
            }
        }

        return false;
    }

    public void callCommand(String tag) {

        // Run a shell file
        try {
            //String[] command = {"/home/ec3user/FlumeSimulator/StaticData/script.sh", tag};
            String[] command = {scriptAbslPath + "/" + scriptName, tag};
            ProcessBuilder processBuilder = new ProcessBuilder(command);

            Process process = processBuilder.start();

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                LOGGER.info("-------------------Success execute the script:" + scriptName);
                //System.exit(0);
            }

        } catch (IOException e) {
            LOGGER.error("------------------- to  execute the script:" + scriptName);
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.error("------------------- to  execute the script:" + scriptName);
            e.printStackTrace();
        }


    }

}
