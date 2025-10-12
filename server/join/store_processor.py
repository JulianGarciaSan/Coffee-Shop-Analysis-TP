from data_processor import DataProcessor


class StoreProcessor(DataProcessor):
    def process_line(self, line: str) -> bool:
        store_id = self.dto_helper.get_column_value(line, 'store_id')
        store_name = self.dto_helper.get_column_value(line, 'store_name')
        
        if store_id and store_name:
            self.data[store_id] = {
                'store_id': store_id,
                'store_name': store_name,
                'raw_line': line
            }
            return True
        return False
