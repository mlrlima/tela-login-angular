package websocket.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebsocketController {

	private final List<String> strings;
	
	public WebsocketController() {
		strings=new ArrayList<>();
	}
	
	@MessageMapping("/add_string")
	@SendTo("/teste/added_string")
	public String addString(@RequestBody String str) {
		strings.add(str);
		return str;
	}
}
