package com.nexus.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexus.messaging.testing.NexusContainers;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Results JSON for the done checks (CLAUDE.md benchmark honesty rules): commit SHA, environment,
 * timestamp, then the check's own parameters and raw measurements. Written only with
 * {@code -PresultsDir=...}.
 */
final class DoneCheckResults {

    private DoneCheckResults() {
    }

    /** Scenario, description, timestamp, commit and environment; the caller adds parameters and measurements. */
    static ObjectNode header(ObjectMapper json, String scenario, String description, Instant now) {
        ObjectNode result = json.createObjectNode();
        result.put("scenario", scenario);
        result.put("description", description);
        result.put("timestamp", now.toString());
        Path repo = Path.of(System.getProperty("nexus.repoRoot"));
        result.put("commitSha", git(repo, "rev-parse", "HEAD"));
        result.put("workingTreeDirty", !git(repo, "status", "--porcelain").isBlank());

        ObjectNode env = result.putObject("environment");
        env.put("kind", "local Testcontainers (tests/e2e)");
        env.put("hostOs", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("hostArch", System.getProperty("os.arch"));
        var docker = DockerClientFactory.instance().client();
        var info = docker.infoCmd().exec();
        env.put("dockerServerVersion", info.getServerVersion());
        env.put("dockerCpus", info.getNCPU());
        env.put("dockerMemoryBytes", info.getMemTotal());
        ObjectNode images = env.putObject("imageArchitectures");
        for (String image : List.of(NexusContainers.POSTGRES_IMAGE, NexusContainers.KAFKA_IMAGE,
                NexusContainers.CONNECT_IMAGE, E2eStack.JRE_IMAGE)) {
            images.put(image, docker.inspectImageCmd(image).exec().getArch());
        }
        env.put("kafkaBrokers", 1);
        env.put("topicPartitions", 12);
        env.put("replicasPerService", 1);
        return result;
    }

    /** Writes {@code <dir>/<scenario>-<UTC stamp>.json} if {@code nexus.resultsDir} is set. */
    static void write(ObjectMapper json, ObjectNode result, Instant now) throws IOException {
        String dir = System.getProperty("nexus.resultsDir");
        if (dir == null) {
            return;
        }
        Path out = Path.of(dir);
        Files.createDirectories(out);
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(now);
        Path file = out.resolve(result.get("scenario").asText() + "-" + stamp + ".json");
        Files.writeString(file, json.copy().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(result) + "\n",
                StandardCharsets.UTF_8);
        System.out.println("Wrote " + file);
    }

    private static String git(Path repo, String... args) {
        List<String> command = new ArrayList<>(List.of("git", "-C", repo.toString()));
        command.addAll(List.of(args));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return process.waitFor() == 0 ? output : "unknown";
        } catch (IOException e) {
            return "unknown";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unknown";
        }
    }
}
