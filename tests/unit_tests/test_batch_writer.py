import asyncio
import json
import unittest
from unittest.mock import *

from batch_writer.batch_processing import (create_table, get_batch_write_items,
                                       get_dynamo_client, save_all_accounts)


class MockEmptyClient():
    def get_object(self, *args, **kwargs):
        return None
    def create_table(self, *args, **kwargs):
        return MagicMock()    

class BatchWriter(unittest.TestCase):

    @patch("boto3.client")
    def test_create_table(self, mock_client):
        mock_client.return_value = MagicMock()
        create_table()
        self.assertTrue(mock_client.return_value.create_table.called)

    @patch("boto3.client")    
    def test_batch_write_records(self, mock_client):        
        mock_client.return_value = MagicMock()        
        try:            
            loop = asyncio.get_event_loop()
            total_records = loop.run_until_complete(save_all_accounts())
            self.assertTrue(mock_client.return_value.batch_write_item.called)
            self.assertEqual(total_records, 9891)
        except KeyboardInterrupt:
            pass
        