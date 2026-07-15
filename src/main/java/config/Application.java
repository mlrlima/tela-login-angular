package config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import security.AuthInterceptor;

@SpringBootApplication(scanBasePackages = {
        "config",
        "controller",
        "security",
        "service"
})
@EntityScan("model")
@EnableJpaRepositories(basePackages = "repository")
public class Application extends SpringBootServletInitializer {

    @Autowired
    private AuthInterceptor authInterceptor;

    public Application() {
        System.out.println(">>> [Application] Construtor chamado - instanciando aplicação...");
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        System.out.println(">>> [Application] configure() chamado - inicializando via Tomcat externo (WAR)...");
        return application.sources(Application.class);
    }

    public static void main(String[] args) {
        System.out.println(">>> [Application] main() chamado - inicializando via Tomcat embutido (JAR)...");
        SpringApplication.run(Application.class, args);
        System.out.println(">>> [Application] Aplicação iniciada com sucesso!");
    }

    @Bean
    public WebMvcConfigurer webMvcConfigurer() {
        System.out.println(">>> [Application] Registrando AuthInterceptor no WebMvcConfigurer...");
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                System.out.println(">>> [Application] AuthInterceptor adicionado ao registry de interceptors.");
                registry.addInterceptor(authInterceptor);
            }
        };
    }
}