package exception;

import java.util.HashMap;
import java.util.Map;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;


//CLASSE: GlobalExceptionHandler
//DESCRICAO: Tratamento global de excecoes para toda a aplicacao
//FUNCAO: Captura excecoes lancadas pelos controllers e retorna
//      respostas padronizadas (evita vazar stack traces)

@ControllerAdvice // Intercepta excecoes de TODOS os controllers
public class GlobalExceptionHandler {
	
	// usuario, empresa, pet nao encontrado etc
	@ExceptionHandler(ResourceNotFoundException.class)
	public ResponseEntity<ErrorResponse> handleResourceNotFound(ResourceNotFoundException ex){
		ErrorResponse erro=new ErrorResponse(
				HttpStatus.NOT_FOUND.value(),
	            ex.getMessage()
	    );
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(erro);
	}
	public static class ResourceNotFoundException extends RuntimeException{
		public ResourceNotFoundException(String mensagem) {
	        super(mensagem);
	    }
	}
	
	
	@ExceptionHandler(UnauthorizedException.class)
	public ResponseEntity<ErrorResponse> handleUnauthorized(UnauthorizedException ex){
		ErrorResponse erro=new ErrorResponse(
				HttpStatus.UNAUTHORIZED.value(),
	            ex.getMessage()
	    );
		return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(erro);
	}
	public static class UnauthorizedException extends RuntimeException{
		public UnauthorizedException(String mensagem) {
	        super(mensagem);
	    }
	}

	
	
    // METODO: handleValidation()
    // FUNCAO: Trata erros de validacao de beans (@NotNull, @Size, @Email, etc.)
    // RETORNO: HTTP 400 (BAD_REQUEST) com mensagem de erro
	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<?> handleValidation(ConstraintViolationException ex) {
		StringBuilder mensagem = new StringBuilder();

		// Percorre todas as violacoes e concatena as mensagens
		for (ConstraintViolation<?> violacao : ex.getConstraintViolations()) {
			if (mensagem.length() > 0) mensagem.append(" ");
			mensagem.append(violacao.getMessage()); // Adiciona a mensagem de erro
		}

		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(Map.of("erro", mensagem.toString()));
	}

    // METODO: handleGeneric()
    // FUNCAO: Captura QUALQUER outra excecao nao tratada
    // RETORNO: HTTP 500 (INTERNAL_SERVER_ERROR) com mensagem generica
	@ExceptionHandler(Exception.class) // Captura TODAS as excecoes
	public ResponseEntity<?> handleGeneric(Exception ex) {
		ex.printStackTrace(); 

		// Cria um mapa com a mensagem generica (NAO expoe detalhes tecnicos)
		Map<String, Object> corpo = new HashMap<>();
		corpo.put("erro", "Erro interno no servidor.");

		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(corpo);
	}
}