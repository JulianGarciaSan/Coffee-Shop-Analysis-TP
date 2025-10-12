from data_processor import DataProcessor


class MenuItemProcessor(DataProcessor):
    def process_line(self, line: str) -> bool:
        item_id = self.dto_helper.get_column_value(line, 'item_id')
        item_name = self.dto_helper.get_column_value(line, 'item_name')
        
        if item_id and item_name:
            self.data[item_id] = {
                'item_id': item_id,
                'item_name': item_name,
            }
            return True
        return False