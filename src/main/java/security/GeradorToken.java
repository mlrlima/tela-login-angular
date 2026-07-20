package security;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import model.Usuario;

//CLASSE: GeradorToken
//DESCRICAO: Gerencia tokens de autenticacao (JWT simplificado)
//FUNCAO: Gera, valida e invalida tokens usando UUID
//ATENCAO: Nao eh JWT - usa armazenamento em memoria (nao escala)
public class GeradorToken {

    // MAPA DE TOKENS (EM MEMORIA)
    // CHAVE: token (String)  -> VALOR: id do usuario (Long)
    // ConcurrentHashMap -> Thread-safe (suporta multiplas requisicoes)
	private static final Map<String, Long> tokens=new ConcurrentHashMap<>();
	
    // METODO: gerar()
    // FUNCAO: Gera um novo token UUID para o usuario
    // RETORNO: String com o token gerado
	public static String gerar(Usuario usuario) {
		// UUID = Universally Unique Identifier (128 bits)
        // Exemplo: 550e8400-e29b-41d4-a716-446655440000
		String token=UUID.randomUUID().toString();
		
		tokens.put(token, usuario.getId());
		
		return token;
	}
	
    // METODO: validar()
    // FUNCAO: Verifica se o token existe e retorna o ID do usuario
    // RETORNO: Long (ID do usuario) ou null (token invalido)
	public static Long validar(String token) {
		if(token==null || token.isEmpty()) return null;
		if(tokens.get(token) ==null) return null;
		
		return tokens.get(token); //retorna o id do usuario
	}
	
    // METODO: invalidar()
    // FUNCAO: Remove o token do mapa (logout)
	public static void invalidar(String token) {
		tokens.remove(token);
	}
	
}
