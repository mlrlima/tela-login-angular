package config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import security.AuthInterceptor;

@SpringBootApplication(scanBasePackages = {
        "controller",
        "config",
        "repository",
        "service",
        "security",
})
@EntityScan("model")
public class Application {

    private final AuthInterceptor authInterceptor;

    public Application(AuthInterceptor authInterceptor) {
        this.authInterceptor = authInterceptor;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public WebMvcConfigurer webMvcConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(authInterceptor);
            }
        };
    }
    
}