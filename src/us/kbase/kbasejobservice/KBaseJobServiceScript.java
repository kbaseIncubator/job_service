package us.kbase.kbasejobservice;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.ini4j.Ini;

import us.kbase.auth.AuthToken;
import us.kbase.auth.TokenFormatException;
import us.kbase.common.service.UObject;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.common.utils.ProcessHelper;
import us.kbase.common.utils.UTCDateFormat;
import us.kbase.common.utils.ProcessHelper.CommandHolder;
import us.kbase.userandjobstate.UserAndJobStateClient;

public class KBaseJobServiceScript {
    private static final String serviceName = KBaseJobServiceServer.SERVICE_NAME;
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.print("Usage: <program> <deploy.cfg> <job_id> <token>");
            System.exit(1);
        }
        File deploy = new File(args[0]);
        if (!deploy.exists())
            throw new IllegalStateException("Configuration file (" + deploy + ") doesn't exist");
        Ini ini = new Ini(deploy);
        Map<String, String> config = ini.get(serviceName);
        if (config == null)
            throw new IllegalStateException("There is no [" + serviceName + "] in configuration file");
        String jobId = args[1];
        String token = args[2];
        KBaseJobServiceClient client = getJobServiceClient(config, token);
        UserAndJobStateClient ujsClient = getUjsClient(config, token);
        try {
            RunJobParams job = client.getJobParams(jobId);
            File jobDir = getJobDir(config, jobId);
            String serviceName = job.getMethod().split("\\.")[0];
            RpcContext context = job.getRpcContext();
            if (context == null)
                context = new RpcContext().withRunId("");
            if (context.getCallStack() == null)
                context.setCallStack(new ArrayList<MethodCall>());
            context.getCallStack().add(new MethodCall().withJobId(jobId).withMethod(job.getMethod())
                    .withTime(new UTCDateFormat().formatDate(new Date())));
            Map<String, Object> rpc = new LinkedHashMap<String, Object>();
            rpc.put("version", "1.1");
            rpc.put("method", job.getMethod());
            rpc.put("params", job.getParams());
            rpc.put("context", job.getRpcContext());
            File inputFile = new File(jobDir, "input.json");
            UObject.getMapper().writeValue(inputFile, rpc);
            String scriptFilePath = getBinScript(config, "run_" + serviceName + "_async_job.sh");
            File outputFile = new File(jobDir, "output.json");
            ujsClient.updateJob(jobId, token, "running", null);
            CommandHolder ch = ProcessHelper.cmd("bash", scriptFilePath, inputFile.getCanonicalPath(),
                    outputFile.getCanonicalPath(), token);
            ch.exec(jobDir);
            FinishJobParams result = UObject.getMapper().readValue(outputFile, FinishJobParams.class);
            client.finishJob(jobId, result);
        } catch (Exception ex) {
            FinishJobParams result = new FinishJobParams().withError(new JsonRpcError().withCode(-1L)
                    .withName("JSONRPCError").withMessage("Job service side error: " + ex.getMessage()));
            try {
                client.finishJob(jobId, result);
            } catch (Exception ignore) {}
        }
    }

    private static KBaseJobServiceClient getJobServiceClient(
            Map<String, String> config, String token)
            throws UnauthorizedException, IOException, MalformedURLException,
            TokenFormatException {
        String jobServiceUrl = config.get(KBaseJobServiceServer.CONFIG_PARAM_CLIENT_JOB_SERVICE_URL);
        if (jobServiceUrl == null)
            throw new IllegalStateException("There is no '" + 
                    KBaseJobServiceServer.CONFIG_PARAM_CLIENT_JOB_SERVICE_URL + "' defined in configuration file");
        KBaseJobServiceClient client = new KBaseJobServiceClient(new URL(jobServiceUrl), new AuthToken(token));
        client.setIsInsecureHttpConnectionAllowed(true);
        return client;
    }
    
    private static File getJobDir(Map<String, String> config, String jobId) {
        String rootDirPath = null;
        String useScratch = config.get(KBaseJobServiceServer.CONFIG_PARAM_CLIENT_USE_SCRATCH_FOR_JOBS);
        if (useScratch != null) {
            useScratch = useScratch.toLowerCase();
            if (useScratch.equals("true") || useScratch.equals("1") ||
                    useScratch.startsWith("y")) {
                rootDirPath = config.get(KBaseJobServiceServer.CONFIG_PARAM_SCRATCH);
            }
        }
        File rootDir = new File(rootDirPath == null ? "." : rootDirPath);
        if (!rootDir.exists())
            rootDir.mkdirs();
        File ret = new File(rootDir, "job_" + jobId);
        if (!ret.exists())
            ret.mkdir();
        return ret;
    }

    private static String getBinScript(Map<String, String> config, String scriptName) {
        File ret = null;
        String binDir = config.get(KBaseJobServiceServer.CONFIG_PARAM_CLIENT_BIN_DIR);
        if (binDir != null) {
            ret = new File(binDir, scriptName);
            if (ret.exists())
                return ret.getAbsolutePath();
        }
        return scriptName;
    }

    private static UserAndJobStateClient getUjsClient(Map<String, String> config, 
            String token) throws Exception {
        String ujsUrl = config.get(KBaseJobServiceServer.CONFIG_PARAM_UJS_URL);
        if (ujsUrl == null)
            throw new IllegalStateException("Parameter '" + 
                    KBaseJobServiceServer.CONFIG_PARAM_UJS_URL + "' is not defined in configuration");
        UserAndJobStateClient ret = new UserAndJobStateClient(new URL(ujsUrl), new AuthToken(token));
        ret.setIsInsecureHttpConnectionAllowed(true);
        return ret;
    }
}
