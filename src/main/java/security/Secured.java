package security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import model.Role;

//ANOTACAO: Secured
//DESCRICAO: Anotacao personalizada para controle de acesso
//FUNCAO: Marca endpoints que requerem autenticacao/autorizacao

@Retention(RetentionPolicy.RUNTIME)
//- A anotacao estara disponivel em tempo de execucao
//- Permite que o AuthInterceptor a leia via reflexao

@Target({ElementType.METHOD, ElementType.TYPE}) //pode ser usada em metodos ou classes
public @interface Secured {
    
	// ATRIBUTO: value()
    // TIPO: Array de Role (enum)
    // DEFAULT: {} (vazio) = qualquer usuario logado
    // ============================================================
    // Exemplos de uso:
    // @Secured                 -> qualquer usuario autenticado
    // @Secured(Role.ADMIN)     -> apenas usuarios com role ADMIN
    // @Secured({Role.ADMIN, Role.USER}) -> ADMIN ou USER
    Role[] value() default {}; // vazio = qualquer usuario logado, independente do role
}
