class UserPurchaseCount:    
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.purchases_qty = 0
    
    def add_purchase(self):
        self.purchases_qty += 1
    
    def to_csv_line(self, store_id: str) -> str:
        return f"{store_id},{self.user_id},{self.purchases_qty}"