# pra rodar: powershell -File war_para_tomcat.ps1

# ================================================
# tela-login-angular - Deploy Completo (mvn clean install)
# Use quando: algo estranho / antes do git push
# ================================================

$tomcatHome = "C:\Users\estagio\Downloads\apache-tomcat-11.0.24\apache-tomcat-11.0.24"
$projectDir = "C:\Users\estagio\tela-login-angular\tela-login-angular"

Write-Host ""
Write-Host "=====================================" -ForegroundColor Magenta
Write-Host "   tela-login-angular - DEPLOY COMPLETO" -ForegroundColor Magenta
Write-Host "=====================================" -ForegroundColor Magenta
Write-Host ""

# 1) Para o Tomcat
Write-Host "[1/5] Parando Tomcat..." -ForegroundColor Yellow
& "$tomcatHome\bin\shutdown.bat" 2>$null
Start-Sleep -Seconds 5

# 2) Git pull
#Write-Host "[2/5] Git pull..." -ForegroundColor Yellow
#Set-Location $projectDir
#git pull

# 3) Clean install completo
Write-Host "[3/5] Compilando (clean install)..." -ForegroundColor Yellow
mvn clean install -DskipTests

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERRO] Build falhou! Subindo Tomcat com versao anterior..." -ForegroundColor Red
    & "$tomcatHome\bin\startup.bat"
    exit 1
}

# 4) Copia o WAR
Write-Host "[4/5] Copiando WAR..." -ForegroundColor Yellow
Remove-Item -Recurse -Force "$tomcatHome\webapps\tela-login-angular"   -ErrorAction SilentlyContinue
Remove-Item -Force          "$tomcatHome\webapps\tela-login-angular.war" -ErrorAction SilentlyContinue
Copy-Item "$projectDir\target\tela-login-angular.war" "$tomcatHome\webapps\tela-login-angular.war"

# 5) Sobe o Tomcat
Write-Host "[5/5] Subindo Tomcat..." -ForegroundColor Yellow
& "$tomcatHome\bin\startup.bat"

Write-Host ""
Write-Host "=====================================" -ForegroundColor Green
Write-Host " Pronto! http://localhost:8080/tela-login-angular" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host ""