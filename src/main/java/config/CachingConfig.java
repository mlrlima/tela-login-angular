package config;

import java.util.Arrays;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CachingConfig {

    @Bean //Cria esse objeto no contexto do Spring
    	//para que outras partes da aplicação possam utilizá-lo.
    public CacheManager cacheManager() {
    	// manager de varios caches
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCaches(Arrays.asList(
        		new ConcurrentMapCache("usuarios"),
                new ConcurrentMapCache("usuarioPorId"),
                new ConcurrentMapCache("empresas"),
                new ConcurrentMapCache("pets")
                ));
        return cacheManager;
    }
}