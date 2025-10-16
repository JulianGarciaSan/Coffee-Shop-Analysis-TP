from data_processor import DataProcessor


class UserProcessor(DataProcessor):
    def process_line(self, line: str) -> bool:
        user_id = self.dto_helper.get_column_value(line, 'user_id')
        birthdate = self.dto_helper.get_column_value(line, 'birthdate')
        
        if user_id and birthdate and user_id != 'user_id':
            clean_user_id = user_id.split('.')[0] if '.' in user_id else user_id
            
            self.data[clean_user_id] = {
                'user_id': clean_user_id,
                'birth_date': birthdate,
                'raw_line': line
            }
            return True
        return False