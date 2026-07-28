package security.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import security.filter.RateLimitingFilter;
import security.filter.SecurityFilter;

//intercepta requisicoes http

@Configuration
@EnableWebSecurity
public class SecurityConfig {

	@Autowired
	SecurityFilter securityFilter;
	
	@Autowired
	RateLimitingFilter rateLimitingFilter;
	
	@Bean
	SecurityFilterChain securityFilterChain(HttpSecurity httpSecurity) throws Exception {
		return httpSecurity
				.csrf(csrf -> csrf.disable()) //Cross Site Request Forgery
				//garante que o Security nunca vai criar uma sessao HTTP e
				// nunca vai usar essa para obter contexto de seguranca
				// todos os requests precisam ser autenticados
				.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
				.authorizeHttpRequests(authorize -> authorize
	                        .requestMatchers(HttpMethod.POST, "/auth/login").permitAll()
	                        .requestMatchers(HttpMethod.POST, "/auth/usuario").permitAll() //criar novo usuario
	                        .requestMatchers("/empresa").hasRole("ADMIN") //apenas ADMIN pode CRUD empresas
	                        .requestMatchers("/empresa/**").hasRole("ADMIN")
	                        .requestMatchers( //para telas do frontend
	                                "/",
	                                "/index.html",
	                                "/*.js",
	                                "/*.css",
	                                "/*.ico"
	                            ).permitAll()
	                        .requestMatchers(HttpMethod.GET, //para fallback
	                                "/{path:^(?!api|assets|.*\\.).*$}",
	                                "/{path:^(?!api|assets|.*\\.).*$}/**"
	                            ).permitAll()
	                        .anyRequest().authenticated() //para o resyo 
	            )
				// rateLimitingFilter -> securityFilter -> UsernamePasswordAuthenticationFilter
				.addFilterBefore(rateLimitingFilter, UsernamePasswordAuthenticationFilter.class)
				.addFilterBefore(securityFilter, UsernamePasswordAuthenticationFilter.class)
				.build();
	}

	@Bean
	AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) throws Exception {
		return authenticationConfiguration.getAuthenticationManager();
	}

	@Bean
	PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}
}
