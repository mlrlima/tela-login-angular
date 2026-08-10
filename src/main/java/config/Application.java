package config;
 
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

//DESCRICAO: Classe principal do Spring Boot
//FUNCAO: Inicializa a aplicacao e configura o contexto

@SpringBootApplication(scanBasePackages = { // Define os pacotes que serao escaneados pelo Spring
        "config", 
        "controller", // Endpoints REST
        "security",
        "service",
        "websocket"
})
@EntityScan("model") // Escaneia as entidades JPA no pacote model
@EnableJpaRepositories(basePackages = "repository") // Habilita e escaneia os repositorios JPA
@EnableScheduling //pra batch/lote as localizacoes pro mapa
public class Application extends SpringBootServletInitializer {
	// Extende SpringBootServletInitializer para permitir deploy em WAR (Tomcat externo)

    public Application() {
        //System.out.println(">>> [Application] Construtor chamado - instanciando aplicação...");
    }

    // FUNCAO: Configura a aplicacao para deploy em WAR (Tomcat externo)
    // CHAMADO QUANDO: A aplicacao eh implantada em um container externo
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        //System.out.println(">>> [Application] configure() chamado - inicializando via Tomcat externo (WAR)...");
        return application.sources(Application.class);
    }

    // FUNCAO: Ponto de entrada da aplicacao (execucao via JAR)
    // CHAMADO QUANDO: A aplicacao roda com 'java -jar' ou 'mvn spring-boot:run'
    public static void main(String[] args) {
        //System.out.println(">>> [Application] main() chamado - inicializando via Tomcat embutido (JAR)...");
        SpringApplication.run(Application.class, args);
        //System.out.println(">>> [Application] Aplicação iniciada com sucesso!");
    }
}