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

//DESCRICAO: Classe principal do Spring Boot
//FUNCAO: Inicializa a aplicacao e configura o contexto

@SpringBootApplication(scanBasePackages = { // Define os pacotes que serao escaneados pelo Spring
        "config", 
        "controller", // Endpoints REST
        "security",
        "service"
})
@EntityScan("model") // Escaneia as entidades JPA no pacote model
@EnableJpaRepositories(basePackages = "repository") // Habilita e escaneia os repositorios JPA
public class Application extends SpringBootServletInitializer {
	// Extende SpringBootServletInitializer para permitir deploy em WAR (Tomcat externo)

    @Autowired //injeta
    private AuthInterceptor authInterceptor;

    public Application() {
        //System.out.println(">>> [Application] Construtor chamado - instanciando aplicação...");
    }

    
    // METODO: configure()
    // FUNCAO: Configura a aplicacao para deploy em WAR (Tomcat externo)
    // CHAMADO QUANDO: A aplicacao eh implantada em um container externo
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        //System.out.println(">>> [Application] configure() chamado - inicializando via Tomcat externo (WAR)...");
        return application.sources(Application.class);
    }

    
    // METODO: main()
    // FUNCAO: Ponto de entrada da aplicacao (execucao via JAR)
    // CHAMADO QUANDO: A aplicacao roda com 'java -jar' ou 'mvn spring-boot:run'
    public static void main(String[] args) {
        //System.out.println(">>> [Application] main() chamado - inicializando via Tomcat embutido (JAR)...");
        SpringApplication.run(Application.class, args);
        //System.out.println(">>> [Application] Aplicação iniciada com sucesso!");
    }

    // METODO: webMvcConfigurer()
    // FUNCAO: Registra o AuthInterceptor para interceptar todas as requisicoes HTTP
    @Bean // Declara este metodo como um Bean gerenciado pelo Spring
    public WebMvcConfigurer webMvcConfigurer() {
        //System.out.println(">>> [Application] Registrando AuthInterceptor no WebMvcConfigurer...");
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                //System.out.println(">>> [Application] AuthInterceptor adicionado ao registry de interceptors.");
                registry.addInterceptor(authInterceptor);
            }
        };
    }
}