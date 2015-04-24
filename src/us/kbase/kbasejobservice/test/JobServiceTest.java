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

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import us.kbase.common.awe.AweClient;
import us.kbase.common.awe.AweResponse;
import us.kbase.common.awe.AwfEnviron;
import us.kbase.common.awe.AwfTemplate;
import us.kbase.common.utils.ProcessHelper;
import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;

public class JobServiceTest {
    public static final String tempDirName = "temp_test";

    @Test
    public void complexTest() throws Exception {
        testShock();
    }
    
    private static void testShock() throws Exception {
        File workDir = prepareWorkDir("shock");
        File mongoDir = new File(workDir, "mongo");
        File shockDir = new File(workDir, "shock");
        File aweServerDir = new File(workDir, "awe_server");
        try {
            int mongoPort = startupMongo(System.getProperty("mongod.path"), mongoDir);
            startupShock(System.getProperty("shock.path"), shockDir, mongoPort);
            startupAweServer(System.getProperty("awe.server.path"), aweServerDir, mongoPort);
        } finally {
            killPid(aweServerDir);
            killPid(shockDir);
            killPid(mongoDir);
        }
    }
    
    private static void runJob(int awePort) throws Exception {
        AweClient client = new AweClient("http://localhost:" + awePort + "/");
        AwfTemplate job = AweClient.createSimpleJobTemplate("job_service", "KBaseTrees.construct_species_tree", "-h", "module_builder");
        AwfEnviron env = new AwfEnviron();
        env.getPrivate().put("KB_AUTH_TOKEN", "secret1");
        job.getTasks().get(0).getCmd().setEnviron(env);
        AweResponse resp = client.submitJob(job);
        System.out.println(new ObjectMapper().writeValueAsString(resp));
        String jobId = resp.getData().getId();
        System.out.println(jobId);
        while (true) {
            Thread.sleep(1000);
            AweResponse resp2 = checkJob(awePort, jobId);
            String state = resp2.getData().getState();
            if ((!state.equals("queuing")) && (!state.equals("in-progress")))
                break;
        }
    }

    private static AweResponse checkJob(int awePort, String jobId) throws IOException {
        InputStream is = new URL("http://localhost:" + awePort + "/job/" + jobId).openStream();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> ret = mapper.readValue(is, Map.class);
        Map<String, Object> data = (Map<String, Object>)ret.get("data");
        List<Map<String, Object>> tasks = (List<Map<String, Object>>)data.get("tasks");
        tasks.get(0).put("predata", null);
        String retText = new ObjectMapper().writeValueAsString(ret);
        System.out.println(retText);
        return mapper.readValue(retText, AweResponse.class);
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

    private static void killPid(File dir) {
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
