#!/usr/bin/env python3
import sys

def write_base_services(f):
    f.write("""services:
  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10
    environment:
      - RABBITMQ_DEFAULT_USER=admin
      - RABBITMQ_DEFAULT_PASS=admin

  client:
    build:
      context: .
      dockerfile: ./client/Dockerfile
    restart: on-failure
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
    depends_on:
      gateway:
        condition: service_started
    volumes:
      - ./client/config.ini:/config.ini
      - ./data/transaction_items:/data/transaction_items
      - ./data/users:/data/users
      - ./data/stores:/data/stores
      - ./data/menu_items:/data/menu_items
      - ./data/payment_methods:/data/payment_methods
      - ./data/vouchers:/data/vouchers
      - ./data/transactions:/data/transactions
      - ./data/transactions_test:/data/transactions_test
      - ./report:/app/report
      - ./data/transactions_items_test:/data/transactions_items_test

  gateway:
    build:
      context: .
      dockerfile: ./server/gateway/Dockerfile
    container_name: gateway
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - OUTPUT_QUEUE=raw_data
      - REPORTS_EXCHANGE=reports_exchange
      - JOIN_EXCHANGE=join.exchange
      - OUTPUT_EXCHANGE=raw_data_exchange
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_filter_year_service(f, instance_id, file_mode, year, nodes_for_year):
    f.write(f"""  filter_year_{instance_id}:
    build:
      context: .
      dockerfile: ./server/filter/Dockerfile
    container_name: filter_year_{instance_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - FILTER_MODE=year
      - INPUT_QUEUE=raw_data
      - OUTPUT_Q1=year_filtered
      - TOTAL_YEAR_FILTERS={nodes_for_year}
      - FILTER_YEARS={year}
      - OUTPUT_Q4=year_filtered_q4
      - OUTPUT_Q2=year_data.exchange
      - INPUT_EXCHANGE=raw_data_exchange
      - FILE_MODE={file_mode}
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_filter_hour_service(f, instance_id, total_hour_filters):
    f.write(f"""  filter_hour_{instance_id}:
    build:
      context: .
      dockerfile: ./server/filter/Dockerfile
    container_name: filter_hour_{instance_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - FILTER_MODE=hour
      - INPUT_QUEUE=year_filtered
      - OUTPUT_Q1=hour_filtered
      - TOTAL_HOUR_FILTERS={total_hour_filters}
      - FILTER_HOURS=06:00-22:59
      - OUTPUT_Q3=groupby.join.exchange
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_filter_amount_service(f, instance_id, total_amount_filters):
    f.write(f"""  filter_amount_{instance_id}:
    build:
      context: .
      dockerfile: ./server/filter/Dockerfile
    container_name: filter_amount_{instance_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - FILTER_MODE=amount
      - INPUT_QUEUE=hour_filtered
      - OUTPUT_Q1=report.exchange
      - TOTAL_AMOUNT_FILTERS={total_amount_filters}
      - MIN_AMOUNT=75
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_groupby_semester_services(f):
    f.write("""  groupby_semester_1:
    build:
      context: .
      dockerfile: ./server/groupby/Dockerfile
    container_name: groupby_semester_1
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - SEMESTER=1
      - INPUT_Q3=groupby.join.exchange
      - TOTAL_GROUPBY_NODES=2
      - JOIN_EXCHANGE=join.exchange
    volumes:
      - ./server/config.ini:/app/config.ini

  groupby_semester_2:
    build:
      context: .
      dockerfile: ./server/groupby/Dockerfile
    container_name: groupby_semester_2
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - SEMESTER=2
      - INPUT_Q3=groupby.join.exchange
      - TOTAL_GROUPBY_NODES=2
      - JOIN_EXCHANGE=join.exchange
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_report_generator_service(f):
    f.write("""  report_generator:
    build:
      context: .
      dockerfile: ./server/report_generator/Dockerfile
    container_name: report_generator
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - INPUT_EXCHANGE=report.exchange
      - REPORTS_EXCHANGE=reports_exchange
    volumes:
      - ./server/config.ini:/app/config.ini
      - ./reports:/app/reports

""")

def write_join_node_service(f):
    f.write("""  join_node:
    build:
      context: .
      dockerfile: ./server/join/Dockerfile
    container_name: join_node
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - INPUT_EXCHANGE=join.exchange
      - OUTPUT_EXCHANGE=report.exchange
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_groupby_best_selling_service(f, instance_id, year, nodes_for_year):
    f.write(f"""  groupby_best_selling_{instance_id}:
    build:
      context: .
      dockerfile: ./server/groupby/Dockerfile
    container_name: groupby_best_selling_{instance_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - GROUPBY_MODE=best_selling
      - INPUT_EXCHANGE=year_data.exchange
      - AGGREGATOR_YEAR={year}
      - OUTPUT_EXCHANGE=best_selling.exchange
      - TOTAL_GROUPBY_NODES={nodes_for_year}
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_aggregator_service(f, year, aggregator_id, aggregators_for_year):
    f.write(f"""  aggregator_year_{year}_{aggregator_id}:
    build:
      context: .
      dockerfile: ./server/aggregator/Dockerfile
    container_name: aggregator_year_{year}_{aggregator_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - AGGREGATOR_MODE=intermediate
      - AGGREGATOR_YEAR={year}
      - INPUT_EXCHANGE=best_selling.exchange
      - OUTPUT_EXCHANGE=best_selling_to_final.exchange
      - TOTAL_INTERMEDIATE_AGGREGATORS={aggregators_for_year}
      - AGGREGATOR_TYPE=best_selling_intermediate
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_aggregator_final_service(f):
    f.write("""  aggregator_best_selling_final:
    build:
      context: .
      dockerfile: ./server/aggregator/Dockerfile
    container_name: aggregator_best_selling_final
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - AGGREGATOR_MODE=final
      - INPUT_EXCHANGE=best_selling_to_final.exchange
      - INPUT_QUEUE=best_selling_final
      - OUTPUT_EXCHANGE=join.exchange
      - TOTAL_INTERMEDIATE=1
      - AGGREGATOR_TYPE=best_selling_final
    volumes:
      - ./server/config.ini:/app/config.ini
""")

def write_top_consumers_aggregator_service(f, aggregator_id, total_top_consumers_aggregators):
    f.write(f"""  top_consumers_aggregator_{aggregator_id}:
    build:
      context: .
      dockerfile: ./server/aggregator/Dockerfile
    container_name: top_consumers_aggregator_{aggregator_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - AGGREGATOR_MODE=intermediate
      - TOPK_NODE_ID={aggregator_id}
      - INPUT_QUEUE=aggregated_data_{aggregator_id}
      - OUTPUT_EXCHANGE=topk.exchange
      - TOTAL_TOPK_NODES={total_top_consumers_aggregators}
      - AGGREGATOR_TYPE=top_consumers_intermediate
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_top_consumers_aggregator_final_service(f, total_top_consumers_aggregators):
    f.write(f"""  top_consumers_aggregator_final:
    build:
      context: .
      dockerfile: ./server/aggregator/Dockerfile
    container_name: top_consumers_aggregator_final
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - AGGREGATOR_MODE=final
      - TOTAL_TOPK_NODES={total_top_consumers_aggregators}
      - INPUT_EXCHANGE=topk.exchange
      - OUTPUT_EXCHANGE=join.exchange
      - AGGREGATOR_TYPE=top_consumers_final
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def write_groupby_top_customers_service(f, instance_id, total_groupby_top_customers):
    f.write(f"""  groupby_top_customers_{instance_id}:
    build:
      context: .
      dockerfile: ./server/groupby/Dockerfile
    container_name: groupby_top_customers_{instance_id}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - GROUPBY_MODE=top_customers
      - INPUT_QUEUE=year_filtered_q4 
      - OUTPUT_EXCHANGE=aggregated.exchange
      - TOTAL_GROUPBY_NODES={total_groupby_top_customers}
    volumes:
      - ./server/config.ini:/app/config.ini

""")

def generate_docker_compose(filename, year_transactions_count, year_transaction_items_count, 
                          hour_count, amount_count, groupby_2024_count, groupby_2025_count,
                          aggregator_2024_count, aggregator_2025_count, groupby_top_customers_count, 
                          top_consumers_aggregator_count):
    
    with open(filename, 'w') as f:
        write_base_services(f)
        
        
        # Filter year services for transactions (2024)
        for i in range(1, year_transactions_count + 1):
            write_filter_year_service(f, i, "transactions", "2024,2025", year_transactions_count)
        
        # Filter year services for transaction_items (2025)
        start_idx = year_transactions_count + 1
        for i in range(start_idx, start_idx + year_transaction_items_count):
            write_filter_year_service(f, i, "transaction_items", "2024,2025", year_transaction_items_count)
        
        # Filter hour services
        for i in range(1, hour_count + 1):
            write_filter_hour_service(f, i, hour_count)
        
        # Groupby semester services (always 2)
        if hour_count > 0:
            write_groupby_semester_services(f)
        
        # Filter amount services
        for i in range(1, amount_count + 1):
            write_filter_amount_service(f, i, amount_count)
        
        # Report generator
        write_report_generator_service(f)
        
        # Join node
        write_join_node_service(f)
        
        # Groupby best selling services
        instance_id = 1
        
        # 2024 nodes
        for _ in range(groupby_2024_count):
            write_groupby_best_selling_service(f, instance_id, 2024, groupby_2024_count)
            instance_id += 1
        
        # 2025 nodes
        for _ in range(groupby_2025_count):
            write_groupby_best_selling_service(f, instance_id, 2025, groupby_2025_count)
            instance_id += 1
        
        # Aggregator services
        
        for i in range(1, aggregator_2024_count + 1):
            write_aggregator_service(f, 2024, i, aggregator_2024_count)
        
        for i in range(1, aggregator_2025_count + 1):
            write_aggregator_service(f, 2025, i, aggregator_2025_count)
        
        # Final aggregator
        write_aggregator_final_service(f)
        
        # Top consumers aggregator services
        for i in range(1, top_consumers_aggregator_count + 1):
            write_top_consumers_aggregator_service(f, i, top_consumers_aggregator_count)
        
        # Top consumers aggregator final
        if top_consumers_aggregator_count > 0:
            write_top_consumers_aggregator_final_service(f, top_consumers_aggregator_count)
        
        # Groupby top customers services
        for i in range(1, groupby_top_customers_count + 1):
            write_groupby_top_customers_service(f, i, groupby_top_customers_count)

def main():
    if len(sys.argv) != 12:
        print("Usage: python configure_nodes.py <filename> <year_transactions> <year_transaction_items> <hour> <amount> <groupby_2024> <groupby_2025> <aggregator_2024> <aggregator_2025> <groupby_top_customers> <top_consumers_aggregator>")
        sys.exit(1)
    
    filename = sys.argv[1]
    year_transactions_count = int(sys.argv[2])
    year_transaction_items_count = int(sys.argv[3])
    hour_count = int(sys.argv[4])
    amount_count = int(sys.argv[5])
    groupby_2024_count = int(sys.argv[6])
    groupby_2025_count = int(sys.argv[7])
    aggregator_2024_count = int(sys.argv[8])
    aggregator_2025_count = int(sys.argv[9])
    groupby_top_customers_count = int(sys.argv[10])
    top_consumers_aggregator_count = int(sys.argv[11])
    
    generate_docker_compose(filename, year_transactions_count, year_transaction_items_count,
                          hour_count, amount_count, groupby_2024_count, groupby_2025_count,
                          aggregator_2024_count, aggregator_2025_count, groupby_top_customers_count, 
                          top_consumers_aggregator_count)
    
    print(f"Docker compose file '{filename}' generated successfully")

if __name__ == "__main__":
    main()