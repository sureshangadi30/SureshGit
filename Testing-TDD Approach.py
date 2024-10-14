import unittest
from datetime import datetime
from your_module import format_birthdate, calculate_age, categorize_salary  # Replace 'your_module' with the actual module name

class TestETLPipeline(unittest.TestCase):

    def test_format_birthdate(self):
        self.assertEqual(format_birthdate('23101992'), '23/10/1992')
        self.assertEqual(format_birthdate('23-10-1992'), '23/10/1992')  # Add more variations if needed

    def test_calculate_age(self):
        birth_date = datetime.strptime('1992-10-23', '%Y-%m-%d')
        self.assertEqual(calculate_age(birth_date), 31)

    def test_categorize_salary(self):
        self.assertEqual(categorize_salary(40000), 'A')
        self.assertEqual(categorize_salary(75000), 'B')
        self.assertEqual(categorize_salary(150000), 'C')
        self.assertEqual(categorize_salary('Invalid'), None)  # Test with invalid input
        
if __name__ == '__main__':
    unittest.main()
