import unittest
from datetime import datetime
from DCS import format_birthdate, calculate_age, categorize_salary

class TestETLPipeline(unittest.TestCase):

    def test_format_birthdate(self):
        self.assertEqual(format_birthdate('23101992'), '23/10/1992')
        self.assertEqual(format_birthdate('23-10-1992'), '23/10/1992')  # Add more variations if needed
        self.assertIsNone(format_birthdate('InvalidDate'))  # Test with an invalid date format

    def test_calculate_age(self):
        birth_date = datetime.strptime('1992-10-23', '%Y-%m-%d')
        self.assertEqual(calculate_age(birth_date), 31)
        self.assertEqual(calculate_age(None), None)  # Test with None input
        
    def test_categorize_salary(self):
        self.assertEqual(categorize_salary(40000), 'A')
        self.assertEqual(categorize_salary(75000), 'B')
        self.assertEqual(categorize_salary(150000), 'C')
        self.assertIsNone(categorize_salary('Invalid'))  # Test with invalid input
        self.assertIsNone(categorize_salary(None))  # Test with None input

    unittest.main()
