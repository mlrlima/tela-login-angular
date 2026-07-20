package security;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

import model.Role;
import model.Usuario;
import repository.UsuarioRepository;

//CLASSE: AuthInterceptor
//DESCRICAO: Intercepta requisicoes HTTP para verificar autenticacao
//FUNCAO: Valida token, carrega usuario e verifica permissoes

@Component // Registra como um Bean gerenciado pelo Spring
public class AuthInterceptor implements HandlerInterceptor {
	// Implementa HandlerInterceptor - permite interceptar requisicoes
    // antes de chegar aos controllers

    private final UsuarioRepository usuarioRepository;

    public AuthInterceptor(UsuarioRepository usuarioRepository) {
        this.usuarioRepository = usuarioRepository;
    }

    
    // METODO: preHandle()
    // FUNCAO: Executado ANTES do controller processar a requisicao
    // RETORNO: true = continua a execucao | false = interrompe
    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws IOException {

    	// PASSO 1: Verifica se o handler eh um metodo de controller
        // Se nao for (ex: recurso estatico), permite o acesso sem validacao
        if (!(handler instanceof HandlerMethod)) {
            return true;
        }

        HandlerMethod hm = (HandlerMethod) handler; //converte de Object para HandlerMethod

        // PASSO 2: Verifica se o metodo ou a classe tem a anotacao @Secured
        Secured secured = hm.getMethodAnnotation(Secured.class); // Verifica no metodo
        if (secured == null) {
            secured = hm.getBeanType().getAnnotation(Secured.class); // Verifica na classe
        }
        if (secured == null) {// Se nao tem @Secured, o endpoint eh publico - permite acesso
            return true;
        }

        // PASSO 3: Valida o token do cabecalho Authorization
        String header = request.getHeader("Authorization");

     // Verifica se o cabecalho existe e esta no formato correto
        if (header == null || !header.startsWith("Bearer ")) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                    "Token ausente"); // HTTP 401 - Nao autorizado
            return false;
        }

        
        // PASSO 4: Valida o token e extrai o ID do usuario
        String token = header.substring(7); // Remove "Bearer " e extrai o token

        Long usuarioId = GeradorToken.validar(token);

        if (usuarioId == null) {// Token invalido, expirado ou manipulado
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                    "Token inválido"); // HTTP 401
            return false;
        }
        
        // PASSO 5: Busca o usuario no banco de dados
        Usuario usuario = usuarioRepository.findById(usuarioId)
        		.orElseThrow(() -> new RuntimeException("Usuario não encontrado"));

        
        // PASSO 6: Verifica se o usuario tem a role/permissao necessaria
        Role[] rolesPermitidas = secured.value(); // Roles permitidas na anotacao

        if (rolesPermitidas.length > 0) { // Se a anotacao especificou roles

            boolean permitido = false;

            for (Role role : rolesPermitidas) { // Verifica se a role do usuario esta na lista de roles permitidas
                if (role == usuario.getRole()) {
                    permitido = true;
                    break;
                }
            }

            if (!permitido) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                        "Sem permissão"); // HTTP 403 - Proibido
                return false;
            }
        }

        // PASSO 7: Usuario autenticado e autorizado - adiciona ao request
        request.setAttribute("usuarioLogado", usuario); // Disponivel para o controller

        return true;
    }
}