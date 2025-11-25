class MessageRangeTracker: 
    def __init__(self):
        self.ranges = []  
    
    def add_message_id(self, message_id: int):
        """
        Agrega un message_id y fusiona rangos adyacentes.
        """
        if not self.ranges:
            self.ranges = [(message_id, message_id)]
            return
        
        new_ranges = []
        inserted = False
        
        for start, end in self.ranges:
            if message_id < start - 1:
                if not inserted:
                    new_ranges.append((message_id, message_id))
                    inserted = True
                new_ranges.append((start, end))
            elif message_id > end + 1:
                new_ranges.append((start, end))
            elif start - 1 <= message_id <= end + 1:
                new_start = min(start, message_id)
                new_end = max(end, message_id)
                
                while new_ranges and new_ranges[-1][1] >= new_start - 1:
                    prev_start, prev_end = new_ranges.pop()
                    new_start = min(prev_start, new_start)
                    new_end = max(prev_end, new_end)
                
                new_ranges.append((new_start, new_end))
                inserted = True
        
        if not inserted:
            new_ranges.append((message_id, message_id))
        
        self.ranges = new_ranges
    
    def contains(self, message_id: int) -> bool:
        """
        Verifica si un message_id está en alguno de los rangos.
        """
        left, right = 0, len(self.ranges) - 1
        
        while left <= right:
            mid = (left + right) // 2
            start, end = self.ranges[mid]
            
            if start <= message_id <= end:
                return True
            elif message_id < start:
                right = mid - 1
            else:
                left = mid + 1
        
        return False
    
    def to_list(self):
        """Serializa rangos a lista para JSON"""
        return self.ranges.copy()
    
    def from_list(self, ranges_list):
        """Deserializa rangos desde lista"""
        self.ranges = ranges_list
    
    def __len__(self):
        """Retorna cantidad de rangos (no de IDs)"""
        return len(self.ranges)
    
    def total_messages(self) -> int:
        """Cuenta total de message_ids cubiertos por los rangos"""
        return sum(end - start + 1 for start, end in self.ranges)
    
