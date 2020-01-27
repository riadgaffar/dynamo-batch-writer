import asyncio

from batch_writer.batch_processing import (ACCOUNT_TABLE, create_table,
                                           list_tables, save_all_accounts)

if __name__== "__main__":    
    tables = list_tables()
    if ACCOUNT_TABLE not in tables.get('TableNames', []):
        create_table()  

    try:        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(save_all_accounts())
    except KeyboardInterrupt:
        pass
