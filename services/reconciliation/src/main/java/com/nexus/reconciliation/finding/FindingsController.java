package com.nexus.reconciliation.finding;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Internal admin API (like {@code /admin/dlq}): never route it through the gateway. */
@RestController
@RequestMapping("/admin/findings")
class FindingsController {

    private final FindingRepository findings;

    FindingsController(FindingRepository findings) {
        this.findings = findings;
    }

    @GetMapping
    List<Finding> list(@RequestParam(defaultValue = "false") boolean open, @RequestParam(defaultValue = "200") int limit) {
        return findings.list(open, Math.min(limit, 10_000));
    }
}
