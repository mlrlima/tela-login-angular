package controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class SpaFallbackController {

	@RequestMapping(value = {
	        "/{path:^(?!api|assets|.*\\.).*$}",
	        "/{path:^(?!api|assets|.*\\.).*$}/**"
	    })
	    public String forward() {
	        return "forward:/index.html";
	    }
}