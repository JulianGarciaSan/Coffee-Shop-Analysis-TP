class TPVAggregation:
    
    def __init__(self):
        self.total_payment_value = 0.0
        self.transaction_count = 0
    
    def add_transaction(self, final_amount: float):
        self.total_payment_value += final_amount
        self.transaction_count += 1
    
    def to_csv_line(self, year_half: str, store_id: str) -> str:
        return f"{year_half},{store_id},{self.total_payment_value:.1f},{self.transaction_count}"