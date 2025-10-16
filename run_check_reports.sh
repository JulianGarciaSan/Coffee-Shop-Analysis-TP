#!/bin/bash

set -e  # Detener si hay algún error

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Detectar modo (test o prod)
MODE="prod"
if [ "$1" == "test" ]; then
    MODE="test"
fi

echo "Iniciando setup para check_reports en modo: $MODE"

# 1. Crear entorno virtual si no existe
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creando entorno virtual...${NC}"
    python3 -m venv venv
    echo -e "${GREEN}Entorno virtual creado${NC}"
else
    echo -e "${GREEN}Entorno virtual ya existe${NC}"
fi

# 2. Activar entorno virtual
echo -e "${YELLOW}Activando entorno virtual...${NC}"
source venv/bin/activate

# 3. Instalar/actualizar dependencias
echo -e "${YELLOW}Instalando dependencias...${NC}"
if [ -f "requirements.txt" ]; then
    pip install --upgrade pip
    pip install -r requirements.txt
    echo -e "${GREEN}Dependencias instaladas${NC}"
else
    echo -e "${RED}No se encontró requirements.txt${NC}"
    echo -e "${YELLOW}Instalando pandas manualmente...${NC}"
    pip install pandas
fi

# 4. Verificar que existan los directorios necesarios
echo -e "${YELLOW}Verificando directorios...${NC}"

if [ "$MODE" == "test" ]; then
    REQUIRED_DIRS=("data/transactions_test" "data/transaction_items_test" "data/users" "data/menu_items" "data/stores" "reports/test")
else
    REQUIRED_DIRS=("data/transactions" "data/transaction_items" "data/users" "data/menu_items" "data/stores" "reports")
fi

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo -e "${RED}Directorio faltante: $dir${NC}"
        echo -e "${YELLOW}Creando directorio...${NC}"
        mkdir -p "$dir"
    fi
done

# Verificar que existan archivos de datos
if [ "$MODE" == "test" ]; then
    TRANSACTIONS_DIR="data/transactions_test"
else
    TRANSACTIONS_DIR="data/transactions"
fi

if [ ! "$(ls -A $TRANSACTIONS_DIR 2>/dev/null)" ]; then
    echo -e "${RED}ADVERTENCIA: No hay archivos en $TRANSACTIONS_DIR${NC}"
fi

# 5. Ejecutar check_reports.py
echo ""
echo -e "${GREEN}=======================================${NC}"
echo -e "${GREEN}Ejecutando check_reports.py en modo: $MODE${NC}"
echo -e "${GREEN}=======================================${NC}"
echo ""

if [ "$MODE" == "test" ]; then
    python3 check_reports.py test
else
    python3 check_reports.py
fi

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo -e "${GREEN}=======================================${NC}"
    echo -e "${GREEN}check_reports.py ejecutado exitosamente${NC}"
    echo -e "${GREEN}=======================================${NC}"
    echo ""
    
    if [ "$MODE" == "test" ]; then
        echo -e "${YELLOW}Reportes generados en ./reports/test/${NC}"
        ls -lh ./reports/test/generated_*.csv 2>/dev/null || echo "No se encontraron reportes generados"
    else
        echo -e "${YELLOW}Reportes generados en ./reports/${NC}"
        ls -lh ./reports/generated_*.csv 2>/dev/null || echo "No se encontraron reportes generados"
    fi
else
    echo ""
    echo -e "${RED}=======================================${NC}"
    echo -e "${RED}Error ejecutando check_reports.py (codigo: $EXIT_CODE)${NC}"
    echo -e "${RED}=======================================${NC}"
fi

# 6. Mantener entorno virtual activo
echo ""
echo -e "${YELLOW}El entorno virtual sigue activo.${NC}"
echo -e "${YELLOW}   Para desactivar: deactivate${NC}"
if [ "$MODE" == "test" ]; then
    echo -e "${YELLOW}   Para ejecutar nuevamente: python3 check_reports.py test${NC}"
else
    echo -e "${YELLOW}   Para ejecutar nuevamente: python3 check_reports.py${NC}"
fi

exit $EXIT_CODE