import unittest
from unittest.mock import MagicMock, patch
import logging
from src.target.PostgreDB import PostgreDB

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class testPostgreDB(unittest.TestCase):

    def setUp(self):
        self.data = [{'email_address': 'example@example.com', 'reviewer_name': 'John Doe'}]
        self.db = PostgreDB(self.data, "reviews")

    @patch('psycopg.connect')
    def test_connect(self, mock_connect):
        self.db.connect()
        mock_connect.assert_called_with(host='localhost', dbname='trustpilot', user='postgres')