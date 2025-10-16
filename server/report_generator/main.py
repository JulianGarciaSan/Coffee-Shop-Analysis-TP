import heapq
import logging
import os
from datetime import datetime
import shutil
import sys
import tempfile
from typing import Optional
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType, ReportBatchDTO
from common.graceful_shutdown import GracefulShutdown

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.report_exchange = os.getenv('REPORT_EXCHANGE', 'report.exchange')
        self.reports_exchange = os.getenv('REPORTS_EXCHANGE', 'reports_exchange')
        
        self.client_data = {}

        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.report_exchange,

            route_keys=['q1.data', 'q3.data', 'q4.data', 'q2_most_profit.data', 'q2_best_selling.data']
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
            
        self.publisher = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.reports_exchange,
                route_keys=['q1.data', 'q3.data', 'q4.data','q2_most_profit.data','q2_best_selling.data']
        )
        
        if hasattr(self.publisher, 'shutdown'):
            self.publisher.shutdown = self.shutdown
            
        self.expected_queries = {'q1': 1,     
                                 'q3': 1,      
                                 'q4': 2,      
                                 'q2_most_profit': 1,   
                                 'q2_best_selling': 1   
                                }
        
        self.csv_files = {}  
        self.eof_received = set() 
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Exchange: {self.report_exchange}")
        logger.info(f"  Queries soportadas: Q1, Q2,Q3, Q4")
 
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en ReportGenerator")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
  
    def process_message(self, message: bytes, routing_key: str, client_id: Optional[int]):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        if client_id is None:
            logger.warning("Mensaje sin client_id, descartando")
            return False
        
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            query_name = routing_key.split('.')[0]
            
            if client_id not in self.client_data:
                self.client_data[client_id] = {
                    'csv_files': {},
                    'eof_count_by_query': {}  
                }
                logger.info(f"Nuevo cliente detectado: {client_id}")
            
            if dto.batch_type == BatchType.EOF:
                eof_counts = self.client_data[client_id]['eof_count_by_query']
                eof_counts[query_name] = eof_counts.get(query_name, 0) + 1
                
                current_count = eof_counts[query_name]
                expected_count = self.expected_queries.get(query_name, 1)
                
                logger.info(f"EOF recibido para cliente {client_id}, query {query_name}: {current_count}/{expected_count}")
                
                if current_count >= expected_count:
                    logger.info(f"Todos los EOFs recibidos para cliente {client_id}, query {query_name} - cerrando archivo")
                    self._close_csv_file(client_id, query_name)
                
                all_complete = True
                for q_name, expected in self.expected_queries.items():
                    received = eof_counts.get(q_name, 0)
                    if received < expected:
                        all_complete = False
                        break
                
                if all_complete:
                    logger.info(f"Cliente {client_id}: Todos los reportes completados")
                    self._publish_reports_for_client(client_id)
                    self._cleanup_client_reports(client_id)
                    del self.client_data[client_id]
                else:
                    progress = []
                    for q_name, expected in self.expected_queries.items():
                        received = eof_counts.get(q_name, 0)
                        progress.append(f"{q_name}:{received}/{expected}")
                    logger.info(f"Cliente {client_id} progreso: {', '.join(progress)}")
                
                return False
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._write_to_csv(dto.data, query_name, client_id)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje para cliente {client_id}: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown solicitado, deteniendo")
            ch.stop_consuming()
            return

        try:
            client_id = None
            if properties and properties.headers:
                raw_client_id = properties.headers.get('client_id')
                if raw_client_id is not None:
                    client_id = int(raw_client_id)
            routing_key = method.routing_key
            should_stop = self.process_message(body, routing_key, client_id)

            if should_stop:
                logger.info("Todos los reportes generados - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}")

    def start(self):
        """Inicia el ReportGenerator."""
        logger.info("Iniciando ReportGenerator...")
        
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            for client_id in list(self.client_data.keys()):
                csv_files = self.client_data[client_id]['csv_files']
                for query_name in list(csv_files.keys()):
                    self._close_csv_file(client_id, query_name)
            
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

    def _initialize_csv_file(self, client_id: int, query_name: str, sample_data: str):
        """Inicializa el archivo CSV con headers para un cliente específico."""
        try:
            reports_dir = f'./reports/client_{client_id}'
            os.makedirs(reports_dir, exist_ok=True)
            os.chmod(reports_dir, 0o755)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{reports_dir}/{query_name}_{timestamp}.csv"
            
            file_handle = open(filename, 'w', encoding='utf-8')
            
            if 'csv_files' not in self.client_data[client_id]:
                self.client_data[client_id]['csv_files'] = {}
            
            self.client_data[client_id]['csv_files'][query_name] = file_handle
            
            header_line = sample_data.strip().split('\n')[0]
            file_handle.write(header_line + '\n')
            file_handle.flush()
            
            logger.info(f"Cliente {client_id}: Archivo CSV inicializado para {query_name}: {filename}")

        except Exception as e:
            logger.error(f"Cliente {client_id}: Error inicializando CSV para {query_name}: {e}")
            raise

    def _write_to_csv(self, csv_data: str, query_name: str, client_id: int):
        """Escribe datos al archivo CSV correspondiente del cliente."""
        try:
            if query_name not in self.client_data[client_id]['csv_files']:
                self._initialize_csv_file(client_id, query_name, csv_data)
            
            lines = csv_data.strip().split('\n')
            file_handle = self.client_data[client_id]['csv_files'][query_name]
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.lower() in [
                    'transaction_id,final_amount',  # Q1 header
                    'year_half_created_at,store_name,tpv',  # Q3 header  
                    'store_name,birthdate',  # Q4 header - SOLO header completo
                    'year_month_created_at,item_name,sellings_qty',  # Q2 best selling header
                    'year_month_created_at,item_name,profit_sum'  # Q2 most profit header
                ]:
                    continue
                
                file_handle.write(line + '\n')
            
            file_handle.flush()
            
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error escribiendo en CSV para {query_name}: {e}")
            raise

    def _close_csv_file(self, client_id: int, query_name: str):
        """Cierra el archivo CSV de un cliente."""
        try:
            if client_id in self.client_data:
                csv_files = self.client_data[client_id]['csv_files']
                if query_name in csv_files:
                    csv_files[query_name].close()
                    if query_name == 'q1':
                        self._sort_q1_file(client_id)  
                    if query_name == 'q4':
                        self._sort_q4_file(client_id) 
                    logger.info(f"Cliente {client_id}: Archivo CSV cerrado para {query_name}")
                    del csv_files[query_name]
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error cerrando CSV para {query_name}: {e}")
            
    def _sort_q1_file(self, client_id: int):
        """Ordena el archivo Q1 por transaction_id usando sorting externo si es necesario."""
        try:
            reports_dir = f'./reports/client_{client_id}'
            files = [f for f in os.listdir(reports_dir) 
                    if f.startswith('q1_') and f.endswith('.csv')]
            
            if not files:
                return
            
            file_path = os.path.join(reports_dir, files[0])
            file_size = os.path.getsize(file_path)
            MAX_MEMORY_SIZE = 100 * 1024 * 1024 
            
            if file_size < MAX_MEMORY_SIZE:
                self._sort_q1_in_memory(file_path)
            else:
                self._sort_q1_external(file_path)
            
            logger.info(f"Cliente {client_id}: Archivo Q1 ordenado ({file_size} bytes)")
            
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error ordenando Q1: {e}")

    def _sort_q1_in_memory(self, file_path: str):
        """Ordena Q1 en memoria (archivos pequeños)."""
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) <= 1:
            return
        
        header = lines[0]
        data_lines = lines[1:]
        
        data_lines.sort(key=lambda line: line.split(',')[0])
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.writelines(data_lines)

    def _sort_q1_external(self, file_path: str):
        """Ordena Q1 usando merge sort externo (archivos grandes)."""
        CHUNK_SIZE = 10000
        temp_files = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                header = f.readline()
                chunk = []
                
                for line in f:
                    chunk.append(line)
                    
                    if len(chunk) >= CHUNK_SIZE:
                        chunk.sort(key=lambda x: x.split(',')[0])
                        temp_file = self._write_temp_chunk(chunk)
                        temp_files.append(temp_file)
                        chunk = []
                
                if chunk:
                    chunk.sort(key=lambda x: x.split(',')[0])
                    temp_file = self._write_temp_chunk(chunk)
                    temp_files.append(temp_file)
            
            self._merge_sorted_chunks_q1(temp_files, file_path, header)
            
        finally:
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass

    def _merge_sorted_chunks_q1(self, temp_files: list, output_path: str, header: str):
        """Merge de chunks ordenados para Q1."""
        file_handles = [open(f, 'r', encoding='utf-8') for f in temp_files]
        
        try:
            heap = []
            
            for i, fh in enumerate(file_handles):
                line = fh.readline()
                if line:
                    transaction_id = line.split(',')[0]
                    heapq.heappush(heap, (transaction_id, line, i))
            
            with open(output_path, 'w', encoding='utf-8') as output:
                output.write(header)
                
                while heap:
                    transaction_id, line, file_idx = heapq.heappop(heap)
                    output.write(line)
                    
                    next_line = file_handles[file_idx].readline()
                    if next_line:
                        next_id = next_line.split(',')[0]
                        heapq.heappush(heap, (next_id, next_line, file_idx))
        
        finally:
            for fh in file_handles:
                fh.close()

    
    def _sort_q4_file(self, client_id: int):
        """Ordena el archivo Q4 por store_name, purchases_qty DESC, birthdate ASC."""
        try:
            reports_dir = f'./reports/client_{client_id}'
            files = [f for f in os.listdir(reports_dir) 
                    if f.startswith('q4_') and f.endswith('.csv')]
            
            if not files:
                return
            
            file_path = os.path.join(reports_dir, files[0])
            file_size = os.path.getsize(file_path)
            MAX_MEMORY_SIZE = 100 * 1024 * 1024 
            
            if file_size < MAX_MEMORY_SIZE:
                self._sort_q4_in_memory(file_path)
            else:
                self._sort_q4_external(file_path)
            
            logger.info(f"Cliente {client_id}: Archivo Q4 ordenado ({file_size} bytes)")
            
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error ordenando Q4: {e}")

    def _sort_q4_in_memory(self, file_path: str):
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) <= 1:
            return
        
        header = lines[0]
        data_lines = lines[1:]
        
        def sort_key(line):
            parts = line.split(',')
            store_name = parts[0]
            birthdate = parts[1] if len(parts) > 1 else ''
            purchases_qty = int(parts[2]) if len(parts) > 2 and parts[2].strip().isdigit() else 0
            return (store_name, -purchases_qty, birthdate)
        
        data_lines.sort(key=sort_key)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.writelines(data_lines)

    def _sort_q4_external(self, file_path: str):
        CHUNK_SIZE = 10000
        temp_files = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                header = f.readline()
                chunk = []
                
                for line in f:
                    chunk.append(line)
                    
                    if len(chunk) >= CHUNK_SIZE:
                        chunk.sort(key=lambda x: self._q4_sort_key(x))
                        temp_file = self._write_temp_chunk(chunk)
                        temp_files.append(temp_file)
                        chunk = []
                
                if chunk:
                    chunk.sort(key=lambda x: self._q4_sort_key(x))
                    temp_file = self._write_temp_chunk(chunk)
                    temp_files.append(temp_file)
            
            self._merge_sorted_chunks_q4(temp_files, file_path, header)
            
        finally:
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass

    def _q4_sort_key(self, line: str):
        parts = line.split(',')
        store_name = parts[0]
        birthdate = parts[1] if len(parts) > 1 else ''
        purchases_qty = int(parts[2]) if len(parts) > 2 and parts[2].strip().isdigit() else 0
        return (store_name, -purchases_qty, birthdate)

    def _merge_sorted_chunks_q4(self, temp_files: list, output_path: str, header: str):
        file_handles = [open(f, 'r', encoding='utf-8') for f in temp_files]
        
        try:
            heap = []
            
            for i, fh in enumerate(file_handles):
                line = fh.readline()
                if line:
                    sort_key = self._q4_sort_key(line)
                    heapq.heappush(heap, (sort_key, line, i))
            
            with open(output_path, 'w', encoding='utf-8') as output:
                output.write(header)
                
                while heap:
                    sort_key, line, file_idx = heapq.heappop(heap)
                    output.write(line)
                    
                    next_line = file_handles[file_idx].readline()
                    if next_line:
                        next_key = self._q4_sort_key(next_line)
                        heapq.heappush(heap, (next_key, next_line, file_idx))
        
        finally:
            for fh in file_handles:
                fh.close()
    
    def _write_temp_chunk(self, chunk: list) -> str:
        """Escribe un chunk en archivo temporal."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                               suffix='.csv', encoding='utf-8')
        temp_file.writelines(chunk)
        temp_file.close()
        return temp_file.name

    def _publish_reports_for_client(self, client_id: int):

        try:
            batch_size = 150
            reports_dir = f'./reports/client_{client_id}'
            
            if not os.path.exists(reports_dir):
                logger.warning(f"Cliente {client_id}: No se encontró directorio de reportes")
                return

            for query_name in self.expected_queries.keys():
                files = [f for f in os.listdir(reports_dir) 
                        if f.startswith(query_name) and f.endswith('.csv')]

                if not files:
                    logger.warning(f"Cliente {client_id}: No se encontraron archivos para {query_name}")
                    continue

                logger.info(f"Cliente {client_id}: Publicando {query_name}, archivos: {files}")
                
                for file in files:
                    file_path = os.path.join(reports_dir, file)
                    
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        total_lines = len(lines)
                        
                        for i in range(0, total_lines, batch_size):
                            batch_lines = lines[i:i+batch_size]
                            batch_data = ''.join(batch_lines)
                            
                            report_dto = ReportBatchDTO(
                                batch_data,
                                BatchType.RAW_CSV,
                                query_name
                            )
                            
                            routing_key = f'client.{client_id}.{query_name}.data'
                            
                            self.publisher.send(
                                report_dto.to_bytes_fast(),
                                routing_key=routing_key
                            )
                        
                        eof_dto = ReportBatchDTO.create_eof(query_name)
                        eof_routing_key = f'client.{client_id}.{query_name}.eof'
                        
                        self.publisher.send(
                            eof_dto.to_bytes_fast(),
                            routing_key=eof_routing_key
                        )
                        
                        logger.info(f"Cliente {client_id}: Enviadas {total_lines} líneas y EOF para {query_name}")

            logger.info(f"Cliente {client_id}: Todos los reportes publicados exitosamente")
            
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error publicando reportes: {e}")

    def _cleanup_client_reports(self, client_id: int):
        try:
            reports_dir = f'./reports/client_{client_id}'
            
            if os.path.exists(reports_dir):
                shutil.rmtree(reports_dir)
                logger.info(f"Cliente {client_id}: Directorio de reportes eliminado: {reports_dir}")
            else:
                logger.warning(f"Cliente {client_id}: Directorio de reportes no existe: {reports_dir}")
                
        except Exception as e:
            logger.error(f"Cliente {client_id}: Error eliminando directorio de reportes: {e}")

if __name__ == "__main__":
    try:
        report_generator = ReportGenerator()
        report_generator.start()
        logger.info("ReportGenerator terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)