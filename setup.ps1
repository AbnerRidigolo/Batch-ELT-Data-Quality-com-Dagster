# PowerShell Automation Script for Dagster Batch Pipeline Setup

# Set script directory
$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
Set-Location -Path $PSScriptRoot

Write-Host "=== INICIANDO SUBIDA DO AMBIENTE DAGSTER BATCH PIPELINE ===" -ForegroundColor Green

# 1. Create project directories
Write-Host "Criando pastas do projeto..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path "data"
New-Item -ItemType Directory -Force -Path "soda"
New-Item -ItemType Directory -Force -Path "dbt_project"
New-Item -ItemType Directory -Force -Path "dagster_project"

# 2. Check and create Python Virtual Environment
if (-not (Test-Path ".venv")) {
    Write-Host "Criando ambiente virtual do Python (.venv)..." -ForegroundColor Cyan
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Falha ao criar o ambiente virtual do Python."
        exit 1
    }
}

# 3. Upgrade pip and install requirements
Write-Host "Instalando dependências do Python no .venv (isso pode levar 1-2 minutos)..." -ForegroundColor Cyan
& ".venv/Scripts/python.exe" -m pip install --upgrade pip
& ".venv/Scripts/pip.exe" install -r requirements.txt

if ($LASTEXITCODE -ne 0) {
    Write-Error "Falha ao instalar os pacotes Python."
    exit 1
}

Write-Host "Instalação concluída com sucesso!" -ForegroundColor Green

Write-Host "`n=== AMBIENTE PRONTO E OPERACIONANTE! ===" -ForegroundColor Green
Write-Host "1. Para rodar a interface do Dagster: .venv/Scripts/dagster dev" -ForegroundColor Cyan
Write-Host "2. Para rodar o Dashboard Streamlit: .venv/Scripts/streamlit run dashboard.py" -ForegroundColor Cyan
