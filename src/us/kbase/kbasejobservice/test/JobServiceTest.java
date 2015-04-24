package us.kbase.kbasejobservice.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import us.kbase.auth.AuthException;
import us.kbase.auth.AuthService;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientCaller;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.ServerException;
import us.kbase.common.service.UObject;
import us.kbase.common.utils.ProcessHelper;
import us.kbase.kbasejobservice.JobState;
import us.kbase.kbasejobservice.KBaseJobServiceClient;
import us.kbase.kbasejobservice.KBaseJobServiceServer;
import us.kbase.kbasejobservice.RunJobParams;
import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;

public class JobServiceTest {
    public static final String tempDirName = "temp_test";
    private static KBaseJobServiceClient client = null;
    private static File workDir = null;
    private static File mongoDir = null;
    private static File shockDir = null;
    private static File aweServerDir = null;
    private static File aweClientDir = null;
    private static File jobServiceDir = null;
    private static Server jobService = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        workDir = prepareWorkDir("integration");
        mongoDir = new File(workDir, "mongo");
        shockDir = new File(workDir, "shock");
        aweServerDir = new File(workDir, "awe_server");
        aweClientDir = new File(workDir, "awe_client");
        jobServiceDir = new File(workDir, "job_service");
        int mongoPort = startupMongo(System.getProperty("mongod.path"), mongoDir);
        int shockPort = startupShock(System.getProperty("shock.path"), shockDir, mongoPort);
        int awePort = startupAweServer(System.getProperty("awe.server.path"), aweServerDir, mongoPort);
        jobService = startupJobService(jobServiceDir, shockPort, awePort);
        int jobServicePort = jobService.getConnectors()[0].getLocalPort();
        startupAweClient(System.getProperty("awe.client.path"), aweClientDir, awePort, jobServiceDir);
        AuthToken token = getToken();
        client = new KBaseJobServiceClient(new URL("http://localhost:" + jobServicePort), token);
        client.setIsInsecureHttpConnectionAllowed(true);
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if (jobService != null) {
            try {
                jobService.stop();
                System.out.println(jobServiceDir.getName() + " was stopped");
            } catch (Exception ignore) {}
        }
        killPid(aweClientDir);
        killPid(aweServerDir);
        killPid(shockDir);
        killPid(mongoDir);
    }

    @Test
    public void testGoodCall() throws Exception {
        String jobId = client.runJob(new RunJobParams().withMethod("SmallTest.parseInt").withParams(Arrays.asList(new UObject("123"))));
        List<Integer> results = waitForJob(client, jobId, new TypeReference<List<Integer>>() {});
        Assert.assertEquals(123, (int)results.get(0));
    }

    @Test
    public void testBadCall() throws Exception {
        String notNumber = "abc_xyz";
        String jobId = client.runJob(new RunJobParams().withMethod("SmallTest.parseInt").withParams(Arrays.asList(new UObject(notNumber))));
        try {
            waitForJob(client, jobId, new TypeReference<List<Integer>>() {});
            Assert.fail("Method is expected to produce an error");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("NumberFormatException") &&
                    ex.getMessage().contains(notNumber));
        }
    }

    private <T> T waitForJob(KBaseJobServiceClient client, String jobId, TypeReference<T> retType)
            throws InterruptedException, IOException, JsonClientException,
            JsonProcessingException {
        for (int i = 0; i < 30; i++) {
            Thread.sleep(1000);
            JobState jobState = client.checkJob(jobId);
            if (jobState.getFinished() == 1L) {
                if (jobState.getError() != null)
                    throw new IllegalStateException("Job error: " + UObject.getMapper().writeValueAsString(jobState.getError()));
                return jobState.getResult().asClassInstance(retType);
            }
        }
        throw new IllegalStateException("Job wasn't finished in 30 seconds");
    }

    private static AuthToken getToken() throws AuthException, IOException {
        String user = System.getProperty("test.user");
        String password = System.getProperty("test.pwd");
        if (user == null || password == null)
            throw new IllegalStateException("Both test.user and test.pwd system properties should be set");
        AuthToken token = AuthService.login(user, password).getToken();
        return token;
    }
    
    private static int startupMongo(String mongodExePath, File dir) throws Exception {
        if (mongodExePath == null)
            mongodExePath = "mongod";
        if (!dir.exists())
            dir.mkdirs();
        File dataDir = new File(dir, "data");
        dataDir.mkdir();
        File logFile = new File(dir, "mongodb.log");
        int port = findFreePort();
        File configFile = new File(dir, "mongod.conf");
        writeFileLines(Arrays.asList(
                "dbpath=" + dataDir.getAbsolutePath(),
                "logpath=" + logFile.getAbsolutePath(),
                "logappend=true",
                "port=" + port,
                "bind_ip=127.0.0.1"
                ), configFile);
        File scriptFile = new File(dir, "start_mongo.sh");
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "cd " + dir.getAbsolutePath(),
                mongodExePath + " --config " + configFile.getAbsolutePath() + " >out.txt 2>err.txt & pid=$!",
                "echo $pid > pid.txt"
                ), scriptFile);
        ProcessHelper.cmd("bash", scriptFile.getCanonicalPath()).exec(dir);
        boolean ready = false;
        for (int n = 0; n < 10; n++) {
            Thread.sleep(1000);
            if (logFile.exists()) {
                if (grep(readFileLines(logFile), "waiting for connections on port " + port).size() > 0) {
                    ready = true;
                    break;
                }
            }
        }
        if (!ready) {
            if (logFile.exists())
                for (String l : readFileLines(logFile))
                    System.err.println("MongoDB log: " + l);
            throw new IllegalStateException("MongoDB couldn't startup in 10 seconds");
        }
        System.out.println(dir.getName() + " was started up");
        return port;
    }

    private static int startupShock(String shockExePath, File dir, int mongoPort) throws Exception {
        if (shockExePath == null)
            shockExePath = "shock-server";
        if (!dir.exists())
            dir.mkdirs();
        File dataDir = new File(dir, "data");
        dataDir.mkdir();
        File logsDir = new File(dir, "logs");
        logsDir.mkdir();
        File siteDir = new File(dir, "site");
        siteDir.mkdir();
        int port = findFreePort();
        File configFile = new File(dir, "shock.cfg");
        writeFileLines(Arrays.asList(
                "[Address]",
                "api-ip=0.0.0.0",
                "api-port=" + port,
                "[Admin]",
                "email=shock-admin@kbase.us",
                "[Anonymous]",
                "read=true",
                "write=true",
                "create-user=false",
                "[Auth]",
                "globus_token_url=https://nexus.api.globusonline.org/goauth/token?grant_type=client_credentials",
                "globus_profile_url=https://nexus.api.globusonline.org/users",
                "[Mongodb]",
                "hosts=localhost:" + mongoPort,
                "database=ShockDBtest",
                "[Mongodb-Node-Indices]",
                "id=unique:true",
                "[Paths]",
                "data=" + dataDir.getAbsolutePath(),
                "logs=" + logsDir.getAbsolutePath(),
                "site=" + siteDir.getAbsolutePath()
                ), configFile);
        File scriptFile = new File(dir, "start_shock.sh");
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "cd " + dir.getAbsolutePath(),
                shockExePath + " --conf " + configFile.getAbsolutePath() + " >out.txt 2>err.txt & pid=$!",
                "echo $pid > pid.txt"
                ), scriptFile);
        ProcessHelper.cmd("bash", scriptFile.getCanonicalPath()).exec(dir);
        Exception err = null;
        for (int n = 0; n < 10; n++) {
            Thread.sleep(1000);
            try {
                BasicShockClient client = new BasicShockClient(new URL("http://localhost:" + port));
                String jsonData = "{\"key\":\"value\"}";
                InputStream is = new ByteArrayInputStream(jsonData.getBytes());
                ShockNode node = client.addNode(new TreeMap<String, Object>(), is, "test.json", "json");
                is.close();
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                client.getFile(node.getId(), os);
                os.close();
                String retJson = new String(os.toByteArray());
                if (jsonData.equals(retJson)) {
                    err = null;
                    break;
                } else {
                    err = new Exception("Shock response doesn't match expected data: " + retJson);
                }
            } catch (Exception ex) {
                err = ex;
            }
        }
        if (err != null) {
            File errorFile = new File(logsDir, "error.log");
            if (errorFile.exists())
                for (String l : readFileLines(errorFile))
                    System.err.println("Shock error: " + l);
            throw new IllegalStateException("Shock couldn't startup in 10 seconds (" + err.getMessage() + ")", err);
        }
        System.out.println(dir.getName() + " was started up");
        return port;
    }

    private static int startupAweServer(String aweServerExePath, File dir, int mongoPort) throws Exception {
        if (aweServerExePath == null)
            aweServerExePath = "awe-server";
        if (!dir.exists())
            dir.mkdirs();
        File dataDir = new File(dir, "data");
        dataDir.mkdir();
        File logsDir = new File(dir, "logs");
        logsDir.mkdir();
        File siteDir = new File(dir, "site");
        siteDir.mkdir();
        File awfDir = new File(dir, "awfs");
        awfDir.mkdir();
        int port = findFreePort();
        File configFile = new File(dir, "awe.cfg");
        writeFileLines(Arrays.asList(
                "[Admin]",
                "email=shock-admin@kbase.us",
                "users=",
                "[Anonymous]",
                "read=true",
                "write=true",
                "delete=true",
                "cg_read=false",
                "cg_write=false",
                "cg_delete=false",
                "[Args]",
                "debuglevel=0",
                "[Auth]",
                "globus_token_url=https://nexus.api.globusonline.org/goauth/token?grant_type=client_credentials",
                "globus_profile_url=https://nexus.api.globusonline.org/users",
                "client_auth_required=false",
                "[Directories]",
                "data=" + dataDir.getAbsolutePath(),
                "logs=" + logsDir.getAbsolutePath(),
                "site=" + siteDir.getAbsolutePath(),
                "awf=" + awfDir.getAbsolutePath(),
                "[Mongodb]",
                "hosts=localhost:" + mongoPort,
                "database=AWEDB",
                "[Mongodb-Node-Indices]",
                "id=unique:true",
                "[Ports]",
                "site-port=" + findFreePort(),
                "api-port=" + port
                ), configFile);
        File scriptFile = new File(dir, "start_awe_server.sh");
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "cd " + dir.getAbsolutePath(),
                aweServerExePath + " --conf " + configFile.getAbsolutePath() + " >out.txt 2>err.txt & pid=$!",
                "echo $pid > pid.txt"
                ), scriptFile);
        ProcessHelper.cmd("bash", scriptFile.getCanonicalPath()).exec(dir);
        Exception err = null;
        for (int n = 0; n < 10; n++) {
            Thread.sleep(1000);
            try {
                InputStream is = new URL("http://localhost:" + port + "/job/").openStream();
                ObjectMapper mapper = new ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> ret = mapper.readValue(is, Map.class);
                if (ret.containsKey("limit") && ret.containsKey("total_count")) {
                    err = null;
                    break;
                } else {
                    err = new Exception("AWE server response doesn't match expected data: " + 
                            mapper.writeValueAsString(ret));
                }
            } catch (Exception ex) {
                err = ex;
            }
        }
        if (err != null) {
            File errorFile = new File(new File(logsDir, "server"), "error.log");
            if (errorFile.exists())
                for (String l : readFileLines(errorFile))
                    System.err.println("AWE server error: " + l);
            throw new IllegalStateException("AWE server couldn't startup in 10 seconds (" + err.getMessage() + ")", err);
        }
        System.out.println(dir.getName() + " was started up");
        return port;
    }

    @SuppressWarnings("unchecked")
    private static int startupAweClient(String aweClientExePath, File dir, int aweServerPort, 
            File jobServiceDir) throws Exception {
        if (aweClientExePath == null)
            aweClientExePath = "awe-client";
        if (!dir.exists())
            dir.mkdirs();
        File dataDir = new File(dir, "data");
        dataDir.mkdir();
        File logsDir = new File(dir, "logs");
        logsDir.mkdir();
        File workDir = new File(dir, "work");
        workDir.mkdir();
        int port = findFreePort();
        File configFile = new File(dir, "awec.cfg");
        writeFileLines(Arrays.asList(
                "[Directories]",
                "data=" + dataDir.getAbsolutePath(),
                "logs=" + logsDir.getAbsolutePath(),
                "[Args]",
                "debuglevel=0",
                "[Client]",
                "workpath=" + workDir.getAbsolutePath(),
                "supported_apps=" + KBaseJobServiceServer.AWE_CLIENT_SCRIPT_NAME,
                "serverurl=http://localhost:" + aweServerPort + "/",
                "group=kbase",
                "name=kbase-client",
                "auto_clean_dir=false",
                "worker_overlap=false",
                "print_app_msg=true",
                "clientgroup_token=",
                "pre_work_script=",
                "pre_work_script_args="
                ), configFile);
        File scriptFile = new File(dir, "start_awe_client.sh");
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "cd " + dir.getAbsolutePath(),
                "export PATH=" + jobServiceDir.getAbsolutePath() + ":$PATH",
                "job_service_run_task.sh >1.out 2>1.err",
                aweClientExePath + " --conf " + configFile.getAbsolutePath() + " >out.txt 2>err.txt & pid=$!",
                "echo $pid > pid.txt"
                ), scriptFile);
        ProcessHelper.cmd("bash", scriptFile.getCanonicalPath()).exec(dir);
        Exception err = null;
        for (int n = 0; n < 10; n++) {
            Thread.sleep(1000);
            try {
                InputStream is = new URL("http://localhost:" + aweServerPort + "/client/").openStream();
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> ret = mapper.readValue(is, Map.class);
                if (ret.containsKey("data") && 
                        ((List<Object>)ret.get("data")).size() > 0) {
                    err = null;
                    break;
                } else {
                    err = new Exception("AWE client response doesn't match expected data: " + 
                            mapper.writeValueAsString(ret));
                }
            } catch (Exception ex) {
                err = ex;
            }
        }
        if (err != null) {
            File errorFile = new File(new File(logsDir, "client"), "error.log");
            if (errorFile.exists())
                for (String l : readFileLines(errorFile))
                    System.err.println("AWE client error: " + l);
            throw new IllegalStateException("AWE client couldn't startup in 10 seconds (" + err.getMessage() + ")", err);
        }
        System.out.println(dir.getName() + " was started up");
        return port;
    }

    private static Server startupJobService(File dir, int shockPort, int awePort) throws Exception {
        Log.setLog(new Logger() {
            @Override
            public void warn(String arg0, Object arg1, Object arg2) {}
            @Override
            public void warn(String arg0, Throwable arg1) {}
            @Override
            public void warn(String arg0) {}
            @Override
            public void setDebugEnabled(boolean arg0) {}
            @Override
            public boolean isDebugEnabled() {
                return false;
            }
            @Override
            public void info(String arg0, Object arg1, Object arg2) {}
            @Override
            public void info(String arg0) {}
            @Override
            public String getName() {
                return null;
            }
            @Override
            public Logger getLogger(String arg0) {
                return this;
            }
            @Override
            public void debug(String arg0, Object arg1, Object arg2) {}
            @Override
            public void debug(String arg0, Throwable arg1) {}
            @Override
            public void debug(String arg0) {}
        });
        if (!dir.exists())
            dir.mkdirs();
        File configFile = new File(dir, "deploy.cfg");
        int port = findFreePort();
        writeFileLines(Arrays.asList(
                "[KBaseJobService]",
                "scratch=" + dir.getAbsolutePath(),
                "ujs.url=http://ci.kbase.us/services/userandjobstate",
                "shock.url=http://localhost:" + shockPort + "/",
                "awe.url=http://localhost:" + awePort + "/",
                "client.job.service.url=http://localhost:" + port + "/",
                "client.use.scratch.for.jobs=false",
                "client.bin.dir=" + dir.getAbsolutePath()
                ), configFile);
        File jobServiceCLI = new File(dir, KBaseJobServiceServer.AWE_CLIENT_SCRIPT_NAME);
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "echo \"$KB_AUTH_TOKEN\"",
                "java -cp " + System.getProperty("java.class.path") + 
                    " us.kbase.kbasejobservice.KBaseJobServiceScript \"" + configFile.getAbsolutePath() + "\" " +
                    "$1 \"$KB_AUTH_TOKEN\""
                ), jobServiceCLI);
        ProcessHelper.cmd("chmod", "a+x", jobServiceCLI.getAbsolutePath()).exec(dir);
        File smallTestCLI = new File(dir, "run_SmallTest_async_job.sh");
        writeFileLines(Arrays.asList(
                "#!/bin/bash",
                "java -cp " + System.getProperty("java.class.path") + " " +
                	"us.kbase.kbasejobservice.test.SmallTestServer $1 $2 $3 >smalltest.out 2> smalltest.err"
                ), smallTestCLI);
        ProcessHelper.cmd("chmod", "a+x", smallTestCLI.getAbsolutePath()).exec(dir);
        System.setProperty("KB_DEPLOYMENT_CONFIG", configFile.getAbsolutePath());
        Server jettyServer = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        jettyServer.setHandler(context);
        context.addServlet(new ServletHolder(new KBaseJobServiceServer()),"/*");
        jettyServer.start();
        Exception err = null;
        JsonClientCaller caller = new JsonClientCaller(new URL("http://localhost:" + port + "/"));
        for (int n = 0; n < 10; n++) {
            Thread.sleep(1000);
            try {
                caller.jsonrpcCall("Unknown", new ArrayList<String>(), null, false, false);
            } catch (ServerException ex) {
                if (ex.getMessage().contains("Can not find method [Unknown] in server class")) {
                    err = null;
                    break;
                } else {
                    err = ex;
                }
            } catch (Exception ex) {
                err = ex;
            }
        }
        if (err != null)
            throw new IllegalStateException("Job service couldn't startup in 10 seconds (" + err.getMessage() + ")", err);
        System.out.println(dir.getName() + " was started up");
        return jettyServer;
    }

    private static void killPid(File dir) {
        if (dir == null)
            return;
        try {
            File pidFile = new File(dir, "pid.txt");
            if (pidFile.exists()) {
                String pid = readFileLines(pidFile).get(0).trim();
                ProcessHelper.cmd("kill", pid).exec(dir);
                System.out.println(dir.getName() + " was stopped");
            }
        } catch (Exception ignore) {}
    }
    
    private static void writeFileLines(List<String> lines, File targetFile) throws IOException {
        PrintWriter pw = new PrintWriter(targetFile);
        for (String l : lines)
            pw.println(l);
        pw.close();
    }

    private static List<String> readFileLines(File f) throws IOException {
        List<String> ret = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(f));
        while (true) {
            String l = br.readLine();
            if (l == null)
                break;
            ret.add(l);
        }
        br.close();
        return ret;
    }

    private static List<String> grep(List<String> lines, String substring) {
        List<String> ret = new ArrayList<String>();
        for (String l : lines)
            if (l.contains(substring))
                ret.add(l);
        return ret;
    }
    
    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {}
        throw new IllegalStateException("Can not find available port in system");
    }
    
    private static File prepareWorkDir(String testName) throws IOException {
        File tempDir = new File(".").getCanonicalFile();
        if (!tempDir.getName().equals(tempDirName)) {
            tempDir = new File(tempDir, tempDirName);
            if (!tempDir.exists())
                tempDir.mkdir();
        }
        for (File dir : tempDir.listFiles()) {
            if (dir.isDirectory() && dir.getName().startsWith("test_" + testName + "_"))
                try {
                    deleteRecursively(dir);
                } catch (Exception e) {
                    System.out.println("Can not delete directory [" + dir.getName() + "]: " + e.getMessage());
                }
        }
        File workDir = new File(tempDir, "test_" + testName + "_" + System.currentTimeMillis());
        if (!workDir.exists())
            workDir.mkdir();
        return workDir;
    }
    
    private static void deleteRecursively(File fileOrDir) {
        if (fileOrDir.isDirectory() && !Files.isSymbolicLink(fileOrDir.toPath()))
            for (File f : fileOrDir.listFiles()) 
                deleteRecursively(f);
        fileOrDir.delete();
    }
}
