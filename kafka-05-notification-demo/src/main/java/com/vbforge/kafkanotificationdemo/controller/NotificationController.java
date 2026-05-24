package com.vbforge.kafkanotificationdemo.controller;

import com.vbforge.kafkanotificationdemo.model.Notification;
import com.vbforge.kafkanotificationdemo.service.NotificationConsumer;
import com.vbforge.kafkanotificationdemo.service.NotificationProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
@RequestMapping("/")
public class NotificationController {

    private final NotificationProducer notificationProducer;
    private final NotificationConsumer notificationConsumer;

    public NotificationController(NotificationProducer notificationProducer, NotificationConsumer notificationConsumer) {
        this.notificationProducer = notificationProducer;
        this.notificationConsumer = notificationConsumer;
    }

    @GetMapping
    public String index(Model model){
        model.addAttribute("notification", new Notification());
        model.addAttribute("processedNotifications", notificationConsumer.getProcessedNotifications());
        return "index";
    }

    @PostMapping("/send")
    public String sendNotification(@ModelAttribute Notification notification) {
        // Generate unique ID if not provided
        if (notification.getId() == null || notification.getId().trim().isEmpty()) {
            notification.setId(UUID.randomUUID().toString());
        }

        // Set default type if not provided
        if (notification.getType() == null || notification.getType().trim().isEmpty()) {
            notification.setType("INFO");
        }

        // Send notification to Kafka
        notificationProducer.sendNotification(notification);

        return "redirect:/";
    }

    @PostMapping("/clear")
    public String clearNotifications() {
        notificationConsumer.clearProcessedNotifications();
        return "redirect:/";
    }

    @GetMapping("/api/notifications")
    @ResponseBody
    public Object getNotifications() {
        return notificationConsumer.getProcessedNotifications();
    }

}
