package websocket.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebsocketController {

    @MessageMapping("/send-message") // App path matches: /app/send-message
    @SendTo("/topic/messages")      // Broadcasts results back to subscribers here
    public String broadcastMessage(String message) {
        return "Server received: " + message;
    }
}
