package org.example;

import java.util.HashMap;
import java.util.Map;

public class AmendLink {
    public static void main(String[] args) {
        
        
        String[] fixMsgs = {
          "35-8|17=N1|55=MSFT|ff=1",
          "35=8|17=A1|19=N1|55=MSFT",
          "35=8|17=A2|19=A1|55=MSFT",
          "35=8|17=A3|19=A2|55=MSFT", 
          "35=8|17=N2|55=MSFT1",
                "35=8|17=N3|55=MSFT2",
                "35=8|17=AA1|19=N2|55=MSFT1"
                
        };
        
        Map<String,String> map = new HashMap<>();
        for (String fixMsg : fixMsgs) {
            String[] tags = fixMsg.split("\\|");
            String currentId = null;
            String prevId = null;

            for (String tag : tags) {
                String[] parts = tag.split("=");
                if (parts.length == 2) {
                    if (parts[0].equals("17")) {
                        currentId = parts[1];
                    } else if (parts[0].equals("19")) {
                        prevId = parts[1];
                    }
                }
            }

            if (currentId != null && prevId != null) {
                map.put(currentId, prevId);
            }
        }

        // Function to get original ID


        // Test cases
        System.out.println("Original ID for A2: " + getOriginalId("A2",map)); // Should print N1
        System.out.println("Original ID for AA1: " + getOriginalId("AA1",map)); // Should print N2
    }
    static String getOriginalId2(String currentId, Map<String,String> map){
        String origId = currentId;
        while (map.containsKey(origId)) {
            origId = map.get(origId);
        }
        return origId;
    }
    static String getOriginalId(String currentId, Map<String,String> map){
        String origId = currentId;
        while (true) {
            String next = map.get(origId);
            if (next == null) break;
            origId = next;
        }
        return origId;
    }


}
