package edu.brown.benchmark;

import org.json.JSONException;
import org.json.JSONObject;

import edu.brown.benchmark.BenchmarkResults.FinalResult;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class JSONResultsPrinter extends ResultsPrinter {
   
    public JSONResultsPrinter(boolean output_clients, boolean output_basepartitions) {
        super(output_clients, output_basepartitions);
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        if (output_basepartitions) {
            System.out.print(StringUtil.SINGLE_LINE);
            System.out.println("Base Partition Distribution:\n" + results.getBasePartitions());
            System.out.print(StringUtil.SINGLE_LINE);
        }
        
        FinalResult fr = new FinalResult(results);
        JSONObject json = null;
        try {
            json = new JSONObject(fr.toJSONString());
            if (output_clients == false) {
                for (String key : CollectionUtil.iterable(json.keys())) {
                    if (key.toLowerCase().startsWith("client")) {
                        json.remove(key);        
                    }
                } // FOR
            }
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        return "<json>\n" + JSONUtil.format(json) + "\n</json>";
    }
}
