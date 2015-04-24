package us.kbase.kbasejobservice.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import us.kbase.common.service.UObject;
import us.kbase.common.service.JsonServerServlet.RpcCallData;

public class SmallTestServer {
    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            processRpcCall(new File(args[0]), new File(args[1]), args[2]);
        } else {
            System.out.println("   or: <program> <context_json_file> <output_json_file> <token>");
            return;
        }
    }

    private static void processRpcCall(File input, File output, String token) {
        OutputStream os = null;
        ObjectMapper mapper = UObject.getMapper();
        try {
            os = new FileOutputStream(output);
            Map<String, Object> rpcCallData = mapper.readValue(input, Map.class);
            String method = (String)rpcCallData.get("method");
            if (!method.equals("SmallTest.parseInt"))
                throw new IllegalStateException("Method is not supported: " + method);
            String param = (String)((List<Object>)rpcCallData.get("params")).get(0);
            int ret = Integer.parseInt(param);
            Map<String, Object> resp = new LinkedHashMap<String, Object>();
            resp.put("version", "1.1");
            resp.put("result", Arrays.asList(ret));
            mapper.writeValue(os, resp);
        } catch (Throwable ex) {
            writeError(mapper, -32400, "Unexpected internal error (" + ex.getMessage() + ")", ex, os);    
        } finally {
            if (os != null)
                try {
                    os.close();
                } catch (Exception ignore) {}
        }
    }

    private static void writeError(ObjectMapper mapper, int code, String message, Throwable ex, OutputStream output) {
        String data = null;
        if (ex != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            pw.close();
            data = sw.toString();
        }
        ObjectNode ret = mapper.createObjectNode();
        ObjectNode error = mapper.createObjectNode();
        error.put("name", "JSONRPCError");
        error.put("code", code);
        error.put("message", message);
        error.put("error", data);
        ret.put("version", "1.1");
        ret.put("error", error);
        try {
            ByteArrayOutputStream bais = new ByteArrayOutputStream();
            mapper.writeValue(bais, ret);
            bais.close();
            output.write(bais.toByteArray());
            output.flush();
        } catch (Exception e) {
            System.err.println(
                    "Unable to write error to output - current exception:");
            e.printStackTrace();
            System.err.println("original exception:");
            ex.printStackTrace();
        }
    }
}
