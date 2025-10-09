#!/bin/bash

if [ $# -ne 1 ]; then
    echo ""
    echo "You can use this script by running: \$ configure_nodes.sh config"
    echo ""
    echo "Usage: $0 <config_file>"
    echo ""
    echo "The config file should contain 11 lines with the following parameters:"
    echo "  1. filename (e.g., docker-compose.yaml)"
    echo "  2. year_transactions"
    echo "  3. year_transaction_items"
    echo "  4. hour"
    echo "  5. amount"
    echo "  6. groupby_2024"
    echo "  7. groupby_2025"
    echo "  8. aggregator_2024"
    echo "  9. aggregator_2025"
    echo "  10. groupby_top_customers"
    echo "  11. top_consumers_aggregator"
    echo ""
    echo "Example config file content:"
    echo "  docker-compose.yaml"
    echo "  2"
    echo "  2"
    echo "  3"
    echo "  3"
    echo "  2"
    echo "  1"
    echo "  1"
    echo "  1"
    echo "  2"
    echo "  2"
    exit 1
fi

CONFIG_FILE=$1

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found"
    exit 1
fi

readarray -t params < "$CONFIG_FILE"

if [ ${#params[@]} -ne 11 ]; then
    echo "Error: Config file must contain exactly 11 lines, found ${#params[@]}"
    exit 1
fi

FILENAME="${params[0]}"
YEAR_TRANSACTIONS="${params[1]}"
YEAR_TRANSACTION_ITEMS="${params[2]}"
HOUR="${params[3]}"
AMOUNT="${params[4]}"
GROUPBY_2024="${params[5]}"
GROUPBY_2025="${params[6]}"
AGGREGATOR_2024="${params[7]}"
AGGREGATOR_2025="${params[8]}"
GROUPBY_TOP_CUSTOMERS="${params[9]}"
TOP_CONSUMERS_AGGREGATOR="${params[10]}"

validate_numeric() {
    local param_name="$1"
    local param_value="$2"
    local line_number="$3"
    
    if ! [[ "$param_value" =~ ^[0-9]+$ ]] || [ "$param_value" -lt 0 ]; then
        echo "Error: Parameter '$param_name' (line $line_number) must be a non-negative integer, got: '$param_value'"
        exit 1
    fi
}

validate_numeric "year_transactions" "$YEAR_TRANSACTIONS" 2
validate_numeric "year_transaction_items" "$YEAR_TRANSACTION_ITEMS" 3
validate_numeric "hour" "$HOUR" 4
validate_numeric "amount" "$AMOUNT" 5
validate_numeric "groupby_2024" "$GROUPBY_2024" 6
validate_numeric "groupby_2025" "$GROUPBY_2025" 7
validate_numeric "aggregator_2024" "$AGGREGATOR_2024" 8
validate_numeric "aggregator_2025" "$AGGREGATOR_2025" 9
validate_numeric "groupby_top_customers" "$GROUPBY_TOP_CUSTOMERS" 10
validate_numeric "top_consumers_aggregator" "$TOP_CONSUMERS_AGGREGATOR" 11

if [ -z "$FILENAME" ]; then
    echo "Error: Filename (line 1) cannot be empty"
    exit 1
fi

echo "Configuration loaded successfully from: $CONFIG_FILE"
echo "Filename: $FILENAME"
echo "Parameters: $YEAR_TRANSACTIONS $YEAR_TRANSACTION_ITEMS $HOUR $AMOUNT $GROUPBY_2024 $GROUPBY_2025 $AGGREGATOR_2024 $AGGREGATOR_2025 $GROUPBY_TOP_CUSTOMERS $TOP_CONSUMERS_AGGREGATOR"

python3 configure_nodes.py "$FILENAME" "$YEAR_TRANSACTIONS" "$YEAR_TRANSACTION_ITEMS" "$HOUR" "$AMOUNT" "$GROUPBY_2024" "$GROUPBY_2025" "$AGGREGATOR_2024" "$AGGREGATOR_2025" "$GROUPBY_TOP_CUSTOMERS" "$TOP_CONSUMERS_AGGREGATOR"

if [ $? -eq 0 ]; then
    echo "To run the services, use:"
    echo "  docker-compose -f $FILENAME up -d --build"
else
    echo "Error generating docker-compose file"
    exit 1
fi