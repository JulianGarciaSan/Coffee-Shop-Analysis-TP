# TP Distribuidos - Coffee Shop Analysis

## Protocolo de Comunicación

### Cliente - Gateway

Se comunicarán usando sockets TCP. El cliente enviará un mensaje de este estilo:

```
<ACTION>|<FILE-TYPE>| <SIZE>  |<LAST-BATCH>|<DATA>
4 bytes |  1 bytes  | 4 bytes |   1 byte   | n bytes
```

Ejemplo:

```
SEND|A|50|0|2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,0.0,38.0,2023-07-01 07:00:00,...\n
SEND|A|50|0|2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,0.0,38.0,2023-07-01 07:00:00,...\n
SEND|A|50|0|2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,0.0,38.0,2023-07-01 07:00:00,...\n
SEND|A|50|0|2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,0.0,38.0,2023-07-01 07:00:00,...\n
SEND|A|50|1|2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,0.0,38.0,2023-07-01 07:00:00,...\n
```

Donde cada `<FILE-TYPE>` representa un tipo de archivo. Los tipos de archivo son:

- **A**: menu_items (item_id, item_name, category, price, is_seasonal, available_from, available_to)
- **B**: payment_methods (payment_method_id, payment_method_name, processing_fee)
- **C**: stores (store_id, store_name, street, postal_code, city, state, latitude)
- **D**: transaction_items (transaction_id, item_id, quantity, unit_price, subtotal, created_at)
- **E**: vouchers (voucher_id, voucher_code, discount_type, discount_value, valid_from, valid_to)
- **F**: users (user_id, gender, birthdate, registered_at)
- **G**: transactions (transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at)

También habrá un mensaje de exit:

```
EXIT
```

A cada mensaje el gateway responderá con un ACK:

```
ACK|<BATCH-ID>|<STATUS>|
4 bytes|4 bytes |1 byte |
```

Donde los códigos de estado son:
```
0 = Success
1 = Retry
2 = Error
```

### Gateway - Workers

Se comunicarán usando RabbitMQ. Se van a definir las siguiente colas por query:

- Query 1:
    - `Gateway->Filter`: Cola de transacciones crudas.
    - `Filter->Filter`: Cola de transacciones del 24 y 25.
    - `Filter->Filter`: Cola de transacciones entre las 6am y 11pm.
    - `Filter->Report`: Cola de transacciones con monto > 75.

- Query 2:
    - `Gateway->Filter`: Cola de transacciones crudas.
    - `Filter->Filter`: Cola de transacciones del 24 y 25.
    - `Filter->GroupBy`: Cola de agrupación de items por mes.
    - Subquery 2-1:
        - `GroupBy->Aggregator`: Count by item más vendido.
        - `Aggregator->Join`: Cola de items más vendidos con metadata.
        - `Join->Report`: Cola de items más vendidos con metadata.
    - Subquery 2-2:
        - `GroupBy->Aggregator`: Sum by item más rentable.
        - `Aggregator->Join`: Cola de items más rentables con metadata.
        - `Join->Report`: Cola de items más rentables con metadata.

- Query 3:
    - `Gateway->Filter`: Cola de transacciones crudas.
    - `Filter->GroupBy`: Cola de transacciones entre las 6am y 11pm.
    - `GroupBy->Join`: Cola de transacciones agrupadas por semestre, año y tienda.
    - `Join->Report`: Cola de transacciones agrupadas por semestre, año y tienda con nombre de tienda.

- Query 4:
    - `Gateway->Filter`: Cola de transacciones crudas.
    - `Filter->Aggregator`: Cola de transacciones del 24 y 25.
    - `Aggregator->Join`: Cola de transacciones contadas por usuario en cada tienda.
    - `Join->Report`: Cola de top 3 usuarios por tienda con metadata.

Vamos a tener muchas colas repetidas entre queries:
    - Cola de transacciones del 24 y 25.
    - Cola de transacciones entre las 6am y 11pm.

Usaremos topics para que los workers puedan suscribirse a las colas que necesiten.
