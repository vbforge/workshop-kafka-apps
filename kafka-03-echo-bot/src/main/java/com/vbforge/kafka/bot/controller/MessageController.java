package com.vbforge.kafka.bot.controller;

import com.vbforge.kafka.bot.service.ConsumerService;
import com.vbforge.kafka.bot.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class MessageController {

    private final ConsumerService consumerService;
    private final ProducerService producerService;

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("recentMessages", consumerService.getRecentMessages());
        return "index";
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message, Model model) {
        producerService.sendMessage(message);

        // NOTE: We do NOT sleep here. The message arrives asynchronously via Kafka.
        // The WebSocket push in ConsumerService handles live delivery to the browser.
        // The page reload just shows the current in-memory list at this moment.

        model.addAttribute("recentMessages", consumerService.getRecentMessages());
        model.addAttribute("lastSent", message);
        return "index";
    }

}
