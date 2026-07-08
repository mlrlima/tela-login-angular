<h1>Tela de login</h1>
<p>
Projeto com finalidade de estudo.
<br> Sistema de gerenciamento de petshop, com autenticação por token, controle de acesso por role e CRUD (Criar, Ler, Atualizar e Deletar) de usuários e pets.
<br> Back-end em Spring MVC expondo uma API REST, consumida por um front-end em Angular (standalone components).
</p>

<h2>Tecnologias utilizadas:</h2>

- Java
- Maven (multi-módulo)
- Tomcat 9
- MySQL
- Spring MVC (API REST)
- Weld (CDI)
- JPA (Java Persistence API)
- Angular (Standalone Components)
- TypeScript

<h2>Telas / Componentes implementados:</h2>

- **Login**: autenticação por e-mail e senha; validação das credenciais cadastradas no banco de dados.
- **Criar usuário**: cadastro de novos usuários; validação para impedir e-mails duplicados.
- **Gestão de usuários**: implementa operações de CRUD; também permite pesquisar usuários pelo nome.
- **Gestão de pets**: implementa operações de CRUD; também permite pesquisar pelo nome; cada pet é associado a um usuário.

<h2>Roles:</h2>

- **ADMIN**: gerenciamento de todos os usuários e pets.
- **USER**: gerenciamento apenas dos seus pets vinculados.

<h2>API REST:</h2>
<p>
Autenticação por token (Bearer/UUID), gerado no login e validado em cada requisição protegida.
<br> As rotas do front-end (Guards) seguem as mesmas regras de role da API: USER só acessa a si mesmo e seus pets; ADMIN acessa tudo.
</p>

- **POST /api/auth/login**: autentica e retorna um token.
- **POST /api/auth/logout**: invalida o token.
- **GET /api/auth/me**: retorna o usuário autenticado.
- **/api/usuario**: CRUD de usuários (cadastro é público; resto exige token).
- **/api/pet**: CRUD de pets (exige token; USER só vê/edita os próprios).

<h2>Estrutura:</h2>

```
backend/src/main/java
├── controller     → Controllers REST (Spring MVC)
├── model          → Entidades JPA
├── repository     → Camada de acesso aos dados
├── security       → Autenticação por token e controle de acesso (@Secured)
├── service        → Regras de negócio
└── util           → Classes utilitárias, transações e EntityManager
```

```
Angular (Standalone Components)
        │
        ▼
HttpClient + AuthInterceptor
        │
        ▼
DispatcherServlet (/api/*)
        │
        ▼
AuthInterceptor (@Secured)
        │
        ▼
Controllers REST
        │
        ▼
Services
        │
        ▼
Repositories
        │
        ▼
JPA (EntityManager)
        │
        ▼
MySQL
```