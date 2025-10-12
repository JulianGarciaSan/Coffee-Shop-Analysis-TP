from data_processor import DataProcessor


class UserProcessor(DataProcessor):
    def process_line(self, line: str) -> bool:
        user_id = self.dto_helper.get_column_value(line, 'user_id')
        birthdate = self.dto_helper.get_column_value(line, 'birthdate')
        gender = self.dto_helper.get_column_value(line, 'gender')
        registered_at = self.dto_helper.get_column_value(line, 'registered_at')
        
        if user_id and birthdate and user_id != 'user_id':
            self.data[user_id] = {
                'user_id': user_id,
                'gender': gender,
                'birth_date': birthdate,
                'registered_at': registered_at,
                'raw_line': line
            }
            return True
        return False