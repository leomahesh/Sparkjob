package com.rta.ec3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestScriptCall {

    public static void main(String[] args) {
        TestScriptCall ts = new TestScriptCall();
        ts.testCall("");
        //ts.testLog();
    }
    public void testCall(String tag) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        // Run a bat file
        processBuilder.command("D:\\test.bat");
        try {

            Process process = processBuilder.start();
            System.out.println("Success!!!!");

            StringBuilder output = new StringBuilder();

           /* BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }*/
            System.out.println("before wait!");
            //int exitVal = process.waitFor();
            /*if (exitVal == 0) {
                System.out.println("Success!");
                //System.out.println(output);
                System.exit(0);
            }*/
            /*System.out.println("after wait!");
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(output);
                //System.exit(0);
            } else {
                //abnormal...
            }*/

        } catch (IOException e) {
            e.printStackTrace();
        } /*catch (InterruptedException e) {
            e.printStackTrace();
        }*/


    }

    public void testLog(){
        try (Stream<String> lines = Files.lines(Paths.get("D:\\logtext.txt"))) {

            // Formatting like \r\n will be lost
            // String content = lines.collect(Collectors.joining());

            // UNIX \n, WIndows \r\n
            String content = lines.collect(Collectors.joining(System.lineSeparator()));
            if(content.contains("ERROR") || content.contains("FATAL")){
                if(content.contains("BusVessel")){
                    System.out.println("Error occured in BusVessel");
                    testCall("BUS");
                }if(content.contains("TaxiLocation")){
                    testCall("TAXILOC");
                    System.out.println("Error occured in TaxiLocation");
                }
            }else{
                //System.out.println(content);
                System.out.println("ok");
            }


            // File to List
            //List<String> list = lines.collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
