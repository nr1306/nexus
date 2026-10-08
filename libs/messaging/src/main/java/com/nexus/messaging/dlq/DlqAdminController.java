package com.nexus.messaging.dlq;

import com.nexus.messaging.dlq.DlqAdmin.DlqMessage;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Admin API for this service's DLQs. Internal only: the gateway (Phase 3) must not route {@code /admin}.
 */
@RestController
@RequestMapping("/admin/dlq")
public class DlqAdminController {

    private final DlqAdmin dlqAdmin;

    public DlqAdminController(DlqAdmin dlqAdmin) {
        this.dlqAdmin = dlqAdmin;
    }

    @GetMapping
    List<String> topics() {
        return dlqAdmin.topics();
    }

    @GetMapping("/{topic}/messages")
    List<DlqMessage> pending(@PathVariable String topic, @RequestParam(defaultValue = "100") int limit) {
        return dlqAdmin.pending(topic, limit);
    }

    @PostMapping("/{topic}/replay")
    Map<String, Integer> replayAll(@PathVariable String topic) {
        return Map.of("replayed", dlqAdmin.replayAll(topic));
    }

    @PostMapping("/{topic}/replay/{partition}/{offset}")
    ResponseEntity<Map<String, Boolean>> replayOne(@PathVariable String topic, @PathVariable int partition,
                                                   @PathVariable long offset) {
        boolean replayed = dlqAdmin.replayOne(topic, partition, offset);
        return replayed ? ResponseEntity.ok(Map.of("replayed", true)) : ResponseEntity.notFound().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> badTopic(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
