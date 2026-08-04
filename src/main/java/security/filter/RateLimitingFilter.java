package security.filter;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class RateLimitingFilter extends OncePerRequestFilter{
	
    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

	private Bucket getBucket(String clientId) {
    	
    	// 10 tokens por minuto
    	Bandwidth limite = Bandwidth.classic(20, Refill.intervally(20, Duration.ofMinutes(1)));
    	
        return buckets.computeIfAbsent(clientId, k -> 
            Bucket.builder()
                  .addLimit(limite)
                  .build()
        );
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException { 
    	
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String clientIp = httpRequest.getRemoteAddr();
        Bucket bucket = getBucket(clientIp);

        if (bucket.tryConsume(1)) {
            filterChain.doFilter(request, response);
        } else {
            response.setStatus(429); // 429 Too Many Requests

            System.out.println("Limite de requisicoes atingido. Tente novamente em alguns minutos.");
            
            return;
        }
    }
    
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        // só aplica rate limit nas rotas da API; deixa passar direto
        // arquivos estáticos, index.html e o fallback da SPA
        
        //System.out.println(path);
        
        if(path.startsWith("/tela-login-angular/auth")
        		|| path.startsWith("/tela-login-angular/empresa")
        		|| path.startsWith("/tela-login-angular/pet")
        		|| path.startsWith("/tela-login-angular/usuario")
        		) return false;
        
        return true;
    }

	
}
