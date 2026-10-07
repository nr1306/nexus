package com.nexus.order;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class OrderApiIT extends OrderIntegrationTest {

    private static final String BODY = """
            {"customerId":"customer-1","currency":"USD","paymentMethod":"pm_card_visa",
             "items":[{"sku":"SKU-0001","quantity":2,"unitPriceCents":1500},
                      {"sku":"SKU-0002","quantity":1,"unitPriceCents":2000}]}
            """;

    @Autowired
    MockMvc mvc;

    @Test
    void placeOrderReturns201AndStartsSaga() throws Exception {
        MvcResult result = mvc.perform(post("/orders").header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotent-Replayed", "false"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.step").value("RESERVE_INVENTORY"))
                .andExpect(jsonPath("$.totalCents").value(5000))
                .andExpect(jsonPath("$.items.length()").value(2))
                .andReturn();

        UUID orderId = orderId(result);
        assertThat(result.getResponse().getHeader("Location")).endsWith("/orders/" + orderId);
        assertThat(commands(orderId)).containsExactly("ReserveInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated");
    }

    @Test
    void replayWithSameKeyReturnsOriginalOrder() throws Exception {
        String key = UUID.randomUUID().toString();

        UUID first = orderId(mvc.perform(post("/orders").header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON).content(BODY)).andReturn());
        MvcResult replay = mvc.perform(post("/orders").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andReturn();

        assertThat(orderId(replay)).isEqualTo(first);
        assertThat(commands(first)).containsExactly("ReserveInventory");
    }

    @Test
    void sameKeyWithDifferentBodyIsRejected() throws Exception {
        String key = UUID.randomUUID().toString();
        mvc.perform(post("/orders").header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isCreated());

        mvc.perform(post("/orders").header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON)
                        .content(BODY.replace("\"quantity\":2", "\"quantity\":3")))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    void concurrentRequestsWithSameKeyCreateOneOrder() throws Exception {
        String key = UUID.randomUUID().toString();
        int requests = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<UUID>> tasks = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            tasks.add(() -> {
                start.await();
                return orderService.placeOrder(key, orderRequest("pm_card_visa")).order().orderId();
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(requests);
        Set<UUID> orderIds = new HashSet<>();
        try {
            List<Future<UUID>> futures = tasks.stream().map(pool::submit).toList();
            start.countDown();
            for (Future<UUID> f : futures) {
                orderIds.add(f.get());
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(orderIds).hasSize(1);
        assertThat(commands(orderIds.iterator().next())).containsExactly("ReserveInventory");
    }

    @Test
    void missingIdempotencyKeyIsBadRequest() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Idempotency-Key header is required"));
    }

    @Test
    void invalidBodiesAreBadRequests() throws Exception {
        List<String> invalid = List.of(
                BODY.replace("\"USD\"", "\"usd\""),
                BODY.replace("\"SKU-0002\"", "\"SKU-0001\""),
                BODY.replace("\"quantity\":2", "\"quantity\":0"),
                BODY.replace("\"unitPriceCents\":1500", "\"unitPriceCents\":-1"),
                """
                {"customerId":"c","currency":"USD","paymentMethod":"pm_card_visa","items":[]}
                """,
                "{not json");
        for (String body : invalid) {
            mvc.perform(post("/orders").header("Idempotency-Key", UUID.randomUUID().toString())
                            .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    void getOrderReturnsStatusAndUnknownIs404() throws Exception {
        UUID orderId = placeOrder();

        mvc.perform(get("/orders/{id}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(orderId.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"));
        mvc.perform(get("/orders/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    private UUID orderId(MvcResult result) throws Exception {
        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(body.get("orderId").asText());
    }
}
