// Controller that manages backlog data and provides both REST and SSE endpoints

package com.mantssm.mantssm.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


@RestController
@RequestMapping("/backlog")
@CrossOrigin(origins = "*")
public class ControllerBacklog {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Stores all active SSE clients
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // Keeps the last sent snapshot of backlogs to compare for changes
    private volatile List<Map<String, Object>> lastSentBacklogs = new ArrayList<>();

    // Constructor starts a scheduled polling task that checks for changes in the database every 15 seconds
    public ControllerBacklog() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            this::checkForUpdates,   // Method to run
            0,          // Initial delay
            15,               // Interval
            TimeUnit.SECONDS);       // Time unit
    }

    // SSE endpoint: clients connect here to receive real-time updates
    @GetMapping("/stream")
    public SseEmitter streamBacklogs() {
        // No timeout (waits indefinitely)
        SseEmitter emitter = new SseEmitter(0L);

        // Add to list of active emitters
        emitters.add(emitter);

        // Remove emitter when it disconnects or errors
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError((ex) -> emitters.remove(emitter));

        return emitter;
    }

    // Called periodically to check if the backlog data has changed
    private void checkForUpdates() {
        try {
            // Query the latest backlog data
            String sql = "SELECT * FROM Export_Backlogs ORDER BY idBackLog DESC";
            List<Map<String, Object>> currentBacklogs = jdbcTemplate.queryForList(sql);

            // If the data has changed since last check, notify all active SSE clients
            if (!currentBacklogs.equals(lastSentBacklogs)) {
                lastSentBacklogs = currentBacklogs;

                for (SseEmitter emitter : emitters) {
                    try {
                        // Send event to client
                        emitter.send(SseEmitter.event()
                            .name("backlog-update")
                            .data(currentBacklogs));
                    } catch (IOException e) {
                        // If the emitter fails, clean it up
                        emitter.complete();
                        emitters.remove(emitter);
                    }
                }
            }

        } catch (Exception ex) {
            // Catch and log any error in the polling process
            System.err.println("Polling failed: " + ex.getMessage());
        }
    }

    // Regular REST endpoint to fetch all backlogs immediately (used for first load)
    @GetMapping(path = "/all")
    public List<Map<String, Object>> getBacklogs() {
        String sql = """
            SELECT * FROM Export_Backlogs ORDER BY idBackLog DESC
        """;
    
        return jdbcTemplate.queryForList(sql);
    }
}
