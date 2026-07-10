package config;

import java.util.HashMap;
import java.util.Map;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

	// erros de validacao do Bean Validation (@NotNull, @Size, @Email, etc.)
	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<?> handleValidation(ConstraintViolationException ex) {
		StringBuilder mensagem = new StringBuilder();

		for (ConstraintViolation<?> violacao : ex.getConstraintViolations()) {
			if (mensagem.length() > 0) mensagem.append(" ");
			mensagem.append(violacao.getMessage());
		}

		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(Map.of("erro", mensagem.toString()));
	}

	// qualquer outra excecao nao tratada -- evita expor stack trace pro cliente
	@ExceptionHandler(Exception.class)
	public ResponseEntity<?> handleGeneric(Exception ex) {
		ex.printStackTrace(); // continua logando no catalina.out pra voce debugar

		Map<String, Object> corpo = new HashMap<>();
		corpo.put("erro", "Erro interno no servidor.");

		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(corpo);
	}
}