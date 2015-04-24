package us.kbase.kbasejobservice;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonServerMethod;
import us.kbase.common.service.JsonServerServlet;

//BEGIN_HEADER

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import us.kbase.common.awe.AweClient;
import us.kbase.common.awe.AweResponse;
import us.kbase.common.awe.AwfEnviron;
import us.kbase.common.awe.AwfTemplate;
import us.kbase.common.service.UObject;
import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;
import us.kbase.shock.client.ShockNodeId;
import us.kbase.userandjobstate.InitProgress;
import us.kbase.userandjobstate.Results;
import us.kbase.userandjobstate.UserAndJobStateClient;

//END_HEADER

/**
 * <p>Original spec-file module name: KBaseJobService</p>
 * <pre>
 * </pre>
 */
public class KBaseJobServiceServer extends JsonServerServlet {
    private static final long serialVersionUID = 1L;

    //BEGIN_CLASS_HEADER
    public static final String SERVICE_NAME = "KBaseJobService";
    public static final String CONFIG_PARAM_SCRATCH = "scratch";
    public static final String CONFIG_PARAM_UJS_URL = "ujs.url";
    public static final String CONFIG_PARAM_SHOCK_URL = "shock.url";
    public static final String CONFIG_PARAM_AWE_URL = "awe.url";
    public static final String CONFIG_PARAM_MAX_JOB_SIZE = "max.job.size";
    public static final String CONFIG_PARAM_CLIENT_JOB_SERVICE_URL = "client.job.service.url";
    public static final String CONFIG_PARAM_CLIENT_USE_SCRATCH_FOR_JOBS = "client.use.scratch.for.jobs";
    public static final String CONFIG_PARAM_CLIENT_BIN_DIR = "client.bin.dir";
    public static final String AWE_CLIENT_SCRIPT_NAME = "job_service_run_task.sh";
    
    private UserAndJobStateClient getUjsClient(AuthToken auth) throws Exception {
        String ujsUrl = config.get(CONFIG_PARAM_UJS_URL);
        if (ujsUrl == null)
            throw new IllegalStateException("Parameter '" + CONFIG_PARAM_UJS_URL +
                    "' is not defined in configuration");
        UserAndJobStateClient ret = new UserAndJobStateClient(new URL(ujsUrl), auth);
        ret.setIsInsecureHttpConnectionAllowed(true);
        return ret;
    }

    private BasicShockClient getShockClient(AuthToken auth) throws Exception {
        String shockUrl = config.get(CONFIG_PARAM_SHOCK_URL);
        if (shockUrl == null)
            throw new IllegalStateException("Parameter '" + CONFIG_PARAM_SHOCK_URL +
                    "' is not defined in configuration");
        BasicShockClient ret = new BasicShockClient(new URL(shockUrl), auth);
        return ret;
    }

    private AweClient getAweClient(AuthToken auth) throws Exception {
        String aweUrl = config.get(CONFIG_PARAM_AWE_URL);
        if (aweUrl == null)
            throw new IllegalStateException("Parameter '" + CONFIG_PARAM_AWE_URL +
                    "' is not defined in configuration");
        AweClient ret = new AweClient(aweUrl);
        return ret;
    }
    
    private long getMaxJobSize() {
        String ret = config.get(CONFIG_PARAM_MAX_JOB_SIZE);
        if (ret == null)
            return 1000000L;
        return Long.parseLong(ret);
    }
    //END_CLASS_HEADER

    public KBaseJobServiceServer() throws Exception {
        super("KBaseJobService");
        //BEGIN_CONSTRUCTOR
        setMaxRPCPackageSize(getMaxJobSize());
        //END_CONSTRUCTOR
    }

    /**
     * <p>Original spec-file function name: run_job</p>
     * <pre>
     * Start a new job
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbasejobservice.RunJobParams RunJobParams}
     * @return   parameter "job_id" of original type "job_id" (A job id.)
     */
    @JsonServerMethod(rpc = "KBaseJobService.run_job")
    public String runJob(RunJobParams params, AuthToken authPart) throws Exception {
        String returnVal = null;
        //BEGIN run_job
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        UObject.getMapper().writeValue(baos, params);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BasicShockClient shockClient = getShockClient(authPart);
        ShockNode shockNode = shockClient.addNode(bais, "job.json", "json");
        UserAndJobStateClient ujsClient = getUjsClient(authPart);
        final String jobId = ujsClient.createAndStartJob(authPart.toString(), "queued", 
                "Automated job for " + params.getMethod(), new InitProgress().withPtype("none"), null);
        ujsClient.setState(SERVICE_NAME, "input:" + jobId, new UObject(shockNode.getId().getId()));
        AweClient client = getAweClient(authPart);
        AwfTemplate job = AweClient.createSimpleJobTemplate(SERVICE_NAME, params.getMethod(), jobId, AWE_CLIENT_SCRIPT_NAME);
        AwfEnviron env = new AwfEnviron();
        env.getPrivate().put("KB_AUTH_TOKEN", authPart.toString());
        job.getTasks().get(0).getCmd().setEnviron(env);
        AweResponse resp = client.submitJob(job);
        String aweJobId = resp.getData().getId();
        ujsClient.setState(SERVICE_NAME, "aweid:" + jobId, new UObject(aweJobId));
        returnVal = jobId;
        //END run_job
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: get_job_params</p>
     * <pre>
     * Get job params necessary for job execution
     * </pre>
     * @param   jobId   instance of original type "job_id" (A job id.)
     * @return   parameter "params" of type {@link us.kbase.kbasejobservice.RunJobParams RunJobParams}
     */
    @JsonServerMethod(rpc = "KBaseJobService.get_job_params")
    public RunJobParams getJobParams(String jobId, AuthToken authPart) throws Exception {
        RunJobParams returnVal = null;
        //BEGIN get_job_params
        UserAndJobStateClient ujsClient = getUjsClient(authPart);
        String shockNodeId = getUjsKeyState(ujsClient, "input:" + jobId).asScalar();
        BasicShockClient shockClient = getShockClient(authPart);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        shockClient.getFile(new ShockNodeId(shockNodeId), baos);
        baos.close();
        returnVal = UObject.getMapper().readValue(
                new ByteArrayInputStream(baos.toByteArray()), RunJobParams.class);
        //END get_job_params
        return returnVal;
    }

    private UObject getUjsKeyState(UserAndJobStateClient ujsClient, String key)
            throws Exception {
        try {
            return ujsClient.getState(SERVICE_NAME, key, 0L);
        } catch (Exception ex) {
            if (ex.getMessage().equals("There is no key " + key))
                return null;
            throw ex;
        }
    }

    /**
     * <p>Original spec-file function name: finish_job</p>
     * <pre>
     * Register results of already started job
     * </pre>
     * @param   jobId   instance of original type "job_id" (A job id.)
     * @param   params   instance of type {@link us.kbase.kbasejobservice.FinishJobParams FinishJobParams}
     */
    @JsonServerMethod(rpc = "KBaseJobService.finish_job")
    public void finishJob(String jobId, FinishJobParams params, AuthToken authPart) throws Exception {
        //BEGIN finish_job
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        UObject.getMapper().writeValue(baos, params);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BasicShockClient shockClient = getShockClient(authPart);
        ShockNode shockNode = shockClient.addNode(bais, "job.json", "json");
        UserAndJobStateClient ujsClient = getUjsClient(authPart);
        ujsClient.setState(SERVICE_NAME, "output:" + jobId, new UObject(shockNode.getId().getId()));
        if (params.getError() == null) {
            ujsClient.completeJob(jobId, authPart.toString(), "done", null, 
                    new Results().withShockurl(config.get(CONFIG_PARAM_SHOCK_URL))
                    .withShocknodes(Arrays.asList(shockNode.getId().getId())));
        } else {
            ujsClient.completeJob(jobId, authPart.toString(), params.getError().getMessage(), params.getError().getError(), null);
        }
        //END finish_job
    }

    /**
     * <p>Original spec-file function name: check_job</p>
     * <pre>
     * Check if job is finished and get results/error
     * </pre>
     * @param   jobId   instance of original type "job_id" (A job id.)
     * @return   parameter "job_state" of type {@link us.kbase.kbasejobservice.JobState JobState}
     */
    @JsonServerMethod(rpc = "KBaseJobService.check_job")
    public JobState checkJob(String jobId, AuthToken authPart) throws Exception {
        JobState returnVal = null;
        //BEGIN check_job
        returnVal = new JobState();
        UserAndJobStateClient ujsClient = getUjsClient(authPart);
        String shockNodeId = null;
        try {
            UObject obj = getUjsKeyState(ujsClient, "output:" + jobId);
            if (obj != null)
                shockNodeId = obj.asScalar();
        } catch (Exception ignore) {}
        if (shockNodeId == null) {
            // We should consult AWE for case the job was killed or gone with no reason.
            String aweJobId = getUjsKeyState(ujsClient, "aweid:" + jobId).asScalar();
            String aweState;
            try {
                InputStream is = new URL(config.get(CONFIG_PARAM_AWE_URL) + "/job/" + aweJobId).openStream();
                ObjectMapper mapper = new ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> aweJob = mapper.readValue(is, Map.class);
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>)aweJob.get("data");
                aweState = (String)data.get("state");
            } catch (Exception ex) {
                throw new IllegalStateException("Error checking AWE job for ujs-id=" + jobId + " (" + ex.getMessage() + ")", ex);
            }
            if ((!aweState.equals("init")) && (!aweState.equals("queued")) && 
                    (!aweState.equals("in-progress")) && (!aweState.equals("completed"))) {
                throw new IllegalStateException("Unexpected job state: " + aweState);
            }
            returnVal.setFinished(0L);
        } else {
            BasicShockClient shockClient = getShockClient(authPart);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            shockClient.getFile(new ShockNodeId(shockNodeId), baos);
            baos.close();
            FinishJobParams result = UObject.getMapper().readValue(
                    new ByteArrayInputStream(baos.toByteArray()), FinishJobParams.class);
            returnVal.setFinished(1L);
            returnVal.setResult(result.getResult());
            returnVal.setError(result.getError());
        }
        //END check_job
        return returnVal;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            new KBaseJobServiceServer().startupServer(Integer.parseInt(args[0]));
        } else {
            System.out.println("Usage: <program> <server_port>");
            return;
        }
    }
}
