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

import security.filter.SecurityFilter;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

	@Autowired
	SecurityFilter securityFilter;
	
	@Bean
	SecurityFilterChain securityFilterChain(HttpSecurity httpSecurity) throws Exception {
		return httpSecurity
				.csrf(csrf -> csrf.disable()) //Cross Site Request Forgery
				.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
				.authorizeHttpRequests(authorize -> authorize
	                        .requestMatchers(HttpMethod.POST, "/auth/login").permitAll()
	                        .requestMatchers(HttpMethod.POST, "/auth/usuario").permitAll() //criar novo usuario
	                        .requestMatchers("/empresa").hasRole("ADMIN") //apenas ADMIN pode CRUD empresas
	                        .requestMatchers("/empresa/**").hasRole("ADMIN")
	                        .requestMatchers(
	                                "/",
	                                "/index.html",
	                                "/*.js",
	                                "/*.css",
	                                "/*.ico"
	                            ).permitAll()
	                        .anyRequest().authenticated() //para o resyo 
	            )
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
