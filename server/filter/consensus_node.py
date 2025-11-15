import socket
import json
import threading
import time
import os
from pathlib import Path

class ConsensusNode:
    def __init__(self, node_id, leader_id, nodes_addresses, log_path, max_entries=10):
        self.node_id = node_id
        self.leader_id = leader_id
        self.is_leader = (node_id == leader_id)
        self.nodes = nodes_addresses
        self.log_path = log_path
        self.max_entries = max_entries
        
        self.log = []
        
        # Lock para acceso concurrente al log
        self.log_lock = threading.Lock()
        
        # Cargar log desde disco si existe
        self._load_log()
        
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.nodes[node_id])
        self.server.listen(10)
        
        print(f"{self.node_id}: Inicializado como {'LÍDER' if self.is_leader else 'FOLLOWER'}")
        print(f"{self.node_id}: Escuchando en {self.nodes[node_id]}")
        print(f"{self.node_id}: Log actual: {len(self.log)} entradas")
        
        threading.Thread(target=self._listen, daemon=True).start()
    
    def _load_log(self):
        try:
            if Path(self.log_path).exists():
                with open(self.log_path, 'r') as f:
                    self.log = [line.strip() for line in f.readlines() if line.strip()]
                print(f"{self.node_id}: Log cargado desde disco ({len(self.log)} entradas)")
        except Exception as e:
            print(f"{self.node_id}: Error cargando log: {e}")
            self.log = []
    
    def _save_log(self):
        try:
            Path(self.log_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.log_path, 'w') as f:
                for entry in self.log:
                    f.write(entry + '\n')
        except Exception as e:
            print(f"{self.node_id}: Error guardando log: {e}")
    
    def _check_duplicate_in_log(self, entry):
        parts = entry.split(':', 2)
        if len(parts) < 3:
            return False, None
        
        dedup_key = f"{parts[1]}:{parts[2]}"  # client_id:message_id
        
        for log_entry in self.log:
            log_parts = log_entry.split(':', 2)
            if len(log_parts) >= 3:
                existing_key = f"{log_parts[1]}:{log_parts[2]}"
                if existing_key == dedup_key:
                    return True, log_parts[0]  # Es duplicado, procesado por log_parts[0]
        
        return False, None
    
    def _listen(self):
        while True:
            try:
                client_socket, client_address = self.server.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except Exception as e:
                print(f"{self.node_id}: Error en listen: {e}")
    
    def _handle_connection(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg['type']
            
            if msg_type == 'CLIENT_REQUEST':
                response = self._handle_client_request(msg)
            elif msg_type == 'APPEND_ENTRIES':
                response = self._handle_append_entries(msg)
            else:
                response = {'error': f'Unknown message type: {msg_type}'}
            
            client_socket.send(json.dumps(response).encode())
            
        except Exception as e:
            print(f"{self.node_id}: Error manejando conexión: {e}")
            error_response = {'error': str(e)}
            try:
                client_socket.send(json.dumps(error_response).encode())
            except:
                pass
        finally:
            client_socket.close()
    
    def _handle_client_request(self, msg):
        if not self.is_leader:
            print(f"No soy líder, redirigiendo a {self.leader_id}")
            return {
                'error': 'Not leader',
                'leader_id': self.leader_id
            }
        
        entry = msg['entry']
        requester = msg.get('requester')  
        
        with self.log_lock:
            is_dup, processed_by = self._check_duplicate_in_log(entry)
            
            if is_dup:
                print(f"{self.node_id}: DUPLICADO detectado - procesado por {processed_by}")
                return {
                    'duplicate': True,
                    'message': f'Entry already processed by {processed_by}'
                }
            
            self.log.append(entry)
            
            if len(self.log) > self.max_entries:
                removed = self.log.pop(0)
                print(f"{self.node_id}: Rotando log, removido: {removed}")
            
            self._save_log()
            
            print(f"{self.node_id}: NUEVO mensaje - {entry} (total: {len(self.log)})")
        
        threading.Thread(
            target=self._replicate_to_followers,
            args=([entry], requester),  # Pasar el requester
            daemon=True
        ).start()
        
        return {
            'duplicate': False,
            'message': 'Entry added to log'
        }
    
    def _handle_append_entries(self, msg):
        entries = msg.get('entries', [])
        
        if not entries:
            return {'success': True}
        
        with self.log_lock:
            added = 0
            for entry in entries:
                if entry not in self.log:
                    self.log.append(entry)
                    added += 1
            
            if len(self.log) > self.max_entries:
                self.log = self.log[-self.max_entries:]
            
            if added > 0:
                self._save_log()
        
        return {'success': True, 'entries_added': added}
    
    def _replicate_to_followers(self, entries, exclude_node=None):
        if not self.is_leader:
            return
        
        msg = {
            'type': 'APPEND_ENTRIES',
            'leader_id': self.leader_id,
            'entries': entries
        }
        
        for node_id in self.nodes:
            if node_id == self.node_id:
                continue
            
            if node_id == exclude_node:
                continue
            
            try:
                self._send_rpc(node_id, msg, timeout=1)
            except Exception as e:
                print(f"{self.node_id}: Error replicando a {node_id}: {e}")
    
    def _send_rpc(self, target_node, msg, timeout=2):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(self.nodes[target_node])
            
            sock.send(json.dumps(msg).encode())
            
            data = sock.recv(4096).decode()
            response = json.loads(data)
            sock.close()
            
            return response  
            
        except socket.timeout:
            print(f"{self.node_id}: Timeout comunicando con {target_node}")
            return None
        except Exception as e:
            print(f"{self.node_id}: Error comunicando con {target_node}: {e}")
            return None
    
    def _ask_leader(self, entry, max_retries=3):
        msg = {
            'type': 'CLIENT_REQUEST',
            'entry': entry,
            'requester': self.node_id
        }
        
        for attempt in range(max_retries):
            response = self._send_rpc(self.leader_id, msg, timeout=2)
            
            if response is None:
                print(f"{self.node_id}: Reintento {attempt + 1}/{max_retries} contactando líder")
                time.sleep(0.5)
                continue
            
            if 'error' in response:
                print(f"{self.node_id}: Error del líder: {response['error']}")
                return True
            
            return response.get('duplicate', False)
        
        print(f"{self.node_id}: ERROR - No pude contactar al líder después de {max_retries} intentos")
        raise Exception(f"No se pudo contactar al líder {self.leader_id}")
    
    def is_duplicate(self, client_id, message_id):
        entry = f"{self.node_id}:{client_id}:{message_id}"
                
        if self.is_leader:
            with self.log_lock:
                is_dup, processed_by = self._check_duplicate_in_log(entry)
                
                if is_dup:
                    print(f"{self.node_id}: DUPLICADO - {client_id}:{message_id} ya procesado por {processed_by}")
                    return True
                
                self.log.append(entry)
                
                if len(self.log) > self.max_entries:
                    removed = self.log.pop(0)
                    print(f"{self.node_id}: Rotando log, removido: {removed}")
                
                self._save_log()
                print(f"{self.node_id}: Log actualizado ({len(self.log)} entradas):")
                for idx, log_entry in enumerate(self.log, 1):
                    print(f"  [{idx}] {log_entry}")
            
            # Replicar a followers
            threading.Thread(
                target=self._replicate_to_followers,
                args=([entry],),
                daemon=True
            ).start()
            
            return False
        
        else:
            # Soy follower: pregunto al líder
            print(f"{self.node_id}: Consultando líder para {entry}")
            return self._ask_leader(entry)