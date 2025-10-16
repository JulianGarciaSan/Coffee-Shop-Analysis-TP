import subprocess
import re
import time
import sys
import datetime
from collections import defaultdict
from typing import Dict, List, Set
from dataclasses import dataclass
import pika


@dataclass
class EOFEvent:
    """Evento del protocolo EOF"""
    timestamp: str
    client_id: str
    event_type: str 
    node_id: str
    details: str = ""


class EOFProtocolLogAnalyzer: 
    def __init__(self):
        self.events: List[EOFEvent] = []
        self.clients_tested: Set[str] = set()
        
        self.patterns = {
            'eof_received': re.compile(r'EOF recibido para cliente (\w+)'),
            'eof_fanout': re.compile(r'EOF_FANOUT publicado para cliente (\w+)'),
            'eof_fanout_received': re.compile(r'EOF_FANOUT recibido para cliente (\w+) de líder (\w+)'),
            'ack_sent': re.compile(r'ACK enviado para cliente (\w+)'),
            'ack_received': re.compile(r'ACK recibido de (\w+) para cliente (\w+)\. Pendientes: (\d+)'),
            'all_acks': re.compile(r'Todos los ACKs recibidos para cliente (\w+)'),
            'eof_propagated': re.compile(r'EOF enviado a Q3 exchange para cliente (\w+)'),
            'leadership': re.compile(r'Tomando liderazgo para cliente (\w+)'),
        }
    
    def run_system_and_collect_logs(self, num_clients: int = 2) -> str:
        subprocess.run(
            ['docker-compose', 'rm', '-fsv', 'filter_hour_1', 'filter_hour_2', 'filter_hour_3'],
            capture_output=True
        )
        time.sleep(2)
        
        result = subprocess.run(
            ['docker-compose', 'up', '-d', '--force-recreate', 
             'filter_hour_1', 'filter_hour_2', 'filter_hour_3'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error levantando contenedores: {result.stderr}")
            sys.exit(1)
        
        time.sleep(15)
        
        start_time = datetime.datetime.now()
        
        try:
            
            credentials = pika.PlainCredentials('admin', 'admin')
            parameters = pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=credentials,
                heartbeat=600
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue='year_filtered', durable=True)
            
            for client_id in range(num_clients):
                num_txs = 20 + (client_id * 10)
                
                lines = [
                    "transaction_id,created_at,store_id,user_id,payment_type,payment_value,promo_code_id,final_amount,datetime"
                ]
                
                for i in range(num_txs):
                    tx_id = f"tx_c{client_id}_{i}"
                    hour = 10 + (i % 10)
                    month = (i % 6) + 1
                    
                    line = (
                        f"{tx_id},"
                        f"2024-{month:02d}-15,"
                        f"store_1,"
                        f"user_1,"
                        f"cash,"
                        f"100.0,"
                        f"promo_1,"
                        f"95.0,"
                        f"2024-{month:02d}-15 {hour:02d}:00:00"
                    )
                    lines.append(line)
                
                csv_data = '\n'.join(lines)
                
                channel.basic_publish(
                    exchange='',
                    routing_key='year_filtered',
                    body=csv_data.encode('utf-8'),
                    properties=pika.BasicProperties(
                        headers={'client_id': str(client_id)},
                        delivery_mode=2
                    )
                )
                
                print(f"Cliente {client_id}: {num_txs} transacciones enviadas")
                time.sleep(0.5)
            
            time.sleep(3)
            
            print("\nEnviando EOF...")
            for client_id in range(num_clients):
                channel.basic_publish(
                    exchange='',
                    routing_key='year_filtered',
                    body=f"EOF:{client_id}".encode('utf-8'),
                    properties=pika.BasicProperties(
                        headers={'client_id': str(client_id)},
                        delivery_mode=2
                    )
                )
                print(f"Cliente {client_id}: EOF enviado")
                time.sleep(0.3)
            
            connection.close()
            
            print("\nEsperando que el protocolo se ejecute...")
            time.sleep(13)
            
        except ImportError:
            print("Error: pika no instalado. Ejecuta: pip install pika")
            sys.exit(1)
        except Exception as e:
            print(f"Error enviando datos: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        since_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        result = subprocess.run(
            ['docker-compose', 'logs', '--since', since_str, 
             'filter_hour_1', 'filter_hour_2', 'filter_hour_3'],
            capture_output=True,
            text=True
        )
        
        logs = result.stdout
        
        print(f"Logs desde: {since_str}")
        print(f"Total: {len(logs.split(chr(10)))} líneas\n")
        
        return logs
    
    def parse_logs(self, logs: str):
        lines = logs.split('\n')
        
        for line in lines:
            node_match = re.match(r'(filter_hour_\d+)\s+\|', line)
            if not node_match:
                continue
            
            node_id = node_match.group(1).replace('filter_', '')
            match = self.patterns['eof_received'].search(line)
            if match:
                client_id = match.group(1)
                self.clients_tested.add(client_id)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='eof_received',
                    node_id=node_id
                ))
                
            match = self.patterns['leadership'].search(line)
            if match:
                client_id = match.group(1)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='leadership',
                    node_id=node_id,
                    details='Se convirtió en líder'
                ))
            
            match = self.patterns['eof_fanout'].search(line)
            if match:
                client_id = match.group(1)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='eof_fanout_sent',
                    node_id=node_id
                ))
            
            match = self.patterns['eof_fanout_received'].search(line)
            if match:
                client_id = match.group(1)
                leader_node = match.group(2)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='eof_fanout_received',
                    node_id=node_id,
                    details=f'De líder {leader_node}'
                ))
            
            match = self.patterns['ack_sent'].search(line)
            if match:
                client_id = match.group(1)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='ack_sent',
                    node_id=node_id
                ))
            
            match = self.patterns['ack_received'].search(line)
            if match:
                from_node = match.group(1)
                client_id = match.group(2)
                pending = match.group(3)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='ack_received',
                    node_id=node_id,
                    details=f'De {from_node}, pendientes: {pending}'
                ))
            
            match = self.patterns['all_acks'].search(line)
            if match:
                client_id = match.group(1)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='all_acks_received',
                    node_id=node_id
                ))
            
            match = self.patterns['eof_propagated'].search(line)
            if match:
                client_id = match.group(1)
                self.events.append(EOFEvent(
                    timestamp='',
                    client_id=client_id,
                    event_type='eof_propagated',
                    node_id=node_id
                ))
    
    def verify_protocol(self) -> bool:
        all_pass = True
        
        for client_id in sorted(self.clients_tested):
            print(f"\nCliente {client_id}:")
            
            client_events = [e for e in self.events if e.client_id == client_id]
            
            leaders = [e for e in client_events if e.event_type == 'leadership']
            if leaders:
                print(f"Líder elegido: {leaders[0].node_id}")
            else:
                print(f"NO se eligió líder")
                all_pass = False
            
            fanout_sent = [e for e in client_events if e.event_type == 'eof_fanout_sent']
            if fanout_sent:
                print(f"EOF_FANOUT enviado por {fanout_sent[0].node_id}")
            else:
                print(f"EOF_FANOUT NO enviado")
                all_pass = False
            
            fanout_received = [e for e in client_events if e.event_type == 'eof_fanout_received']
            if len(fanout_received) >= 1:
                print(f"EOF_FANOUT recibido por {len(fanout_received)} nodos")
                for e in fanout_received:
                    print(f"     • {e.node_id}: {e.details}")
            else:
                print(f"EOF_FANOUT recibido por pocos nodos")
            
            acks_sent = [e for e in client_events if e.event_type == 'ack_sent']
            if len(acks_sent) >= 1:
                print(f"{len(acks_sent)} ACKs enviados")
                for e in acks_sent:
                    print(f"     • {e.node_id}")
            else:
                print(f"NO se enviaron ACKs")
                all_pass = False
            
            acks_received = [e for e in client_events if e.event_type == 'ack_received']
            if acks_received:
                print(f"Líder recibió {len(acks_received)} ACKs")
                for e in acks_received:
                    print(f"     • {e.details}")
            else:
                print(f"Líder NO recibió ACKs")
                all_pass = False
            
            all_acks = [e for e in client_events if e.event_type == 'all_acks_received']
            if all_acks:
                print(f"Todos los ACKs recibidos (nodo {all_acks[0].node_id})")
            else:
                print(f"NO se recibieron todos los ACKs")
                all_pass = False
            
            eof_prop = [e for e in client_events if e.event_type == 'eof_propagated']
            if eof_prop:
                print(f"EOF propagado downstream por {eof_prop[0].node_id}")
            else:
                print(f"EOF NO propagado")
                all_pass = False
            
            all_acks_idx = next((i for i, e in enumerate(client_events) if e.event_type == 'all_acks_received'), None)
            eof_prop_idx = next((i for i, e in enumerate(client_events) if e.event_type == 'eof_propagated'), None)
            
            if all_acks_idx is not None and eof_prop_idx is not None:
                if all_acks_idx < eof_prop_idx:
                    print(f"Orden correcto: Todos ACKs → EOF propagado")
                else:
                    print(f"Orden incorrecto: EOF propagado antes de recibir todos los ACKs")
                    all_pass = False
        
        print("\n" + "="*70)
        if all_pass:
            print("protocolo de EOF correcto")
        else:
            print("PROTOCOLO CON ERRORES")
        print("="*70 + "\n")
        
        return all_pass
    
    def run_test(self):
        try:
            logs = self.run_system_and_collect_logs(num_clients=2)
            
            if logs is None or len(logs.strip()) == 0:
                print("Error: No se pudieron recolectar logs")
                return False
            
            self.parse_logs(logs)
            
            success = self.verify_protocol()
            
            return success
            
        except Exception as e:
            print(f"\nError durante el test: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    analyzer = EOFProtocolLogAnalyzer()
    
    try:
        success = analyzer.run_test()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrumpido por usuario")
        sys.exit(1)


if __name__ == "__main__":
    main()