@echo off
echo ========================================
echo INSTALADOR - PROJETO TL - PNAD
echo ========================================
echo.

echo Verificando se Python esta instalado...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado!
    echo Por favor, instale o Python 3.8+ primeiro
    echo Baixe em: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo Python encontrado!
python --version
echo.

echo Instalando dependencias...
pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo ERRO na instalacao das dependencias!
    echo Tente executar: pip install -r requirements.txt
    pause
    exit /b 1
)

echo.
echo ========================================
echo INSTALACAO CONCLUIDA COM SUCESSO!
echo ========================================
echo.
echo Para testar a conexao:
echo   python test_connection.py
echo.
echo Para executar a aplicacao:
echo   python app.py
echo.
echo Para acessar a API:
echo   http://localhost:8000
echo.
pause 