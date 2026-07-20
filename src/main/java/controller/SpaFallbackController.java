package controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

//CLASSE: SpaFallbackController
//DESCRICAO: Redireciona requisicoes de rotas do Angular para o index.html
//FUNCAO: Suporte a Single Page Application (SPA) - permite refresh de paginas

@Controller // Indica que eh um controller MVC (NAO REST)
public class SpaFallbackController {

    // METODO: forward()
    // FUNCAO: Redireciona todas as requisicoes que NAO sao API ou assets
    //         para o index.html (para o Angular cuidar do roteamento)
	@RequestMapping(value = {
            // Expressao regular: captura qualquer caminho que NAO comece com:
            // - api/    (endpoints da API)
            // - assets/ (arquivos estaticos)
            // - .       (arquivos com extensao: .js, .css, .png, etc.)
	        "/{path:^(?!api|assets|.*\\.).*$}",
	        "/{path:^(?!api|assets|.*\\.).*$}/**" // Sub-caminhos tambem
	    })
	    public String forward() {
	        return "forward:/index.html";
	    }
}