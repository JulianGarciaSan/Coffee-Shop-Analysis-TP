# TP Distribuidos - Coffee Shop Analysis

## Ejecucion de pruebas end of file
### Crear entorno virtual
python3 -m venv venv

### Activar
source venv/bin/activate

### Instalar pika
pip install pika

### Ejecutar el test
python3 tests/eof_protocol/test_eof_protocol.py 

### Trackear logs
docker-compose logs groupby_semester_1 groupby_semester_2| tail -100


### Cargar reportes de Pandas como la catedra
chmod +x setup_and_run.sh

Set reducido
./setup_and_run.sh test 

Set completo
./setup_and_run.sh

### Luego para comparar los reportes

python3 check_results_final.py 1

Donde el numero del final representa el id del cliente


