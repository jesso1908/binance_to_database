import asyncio
from binance import AsyncClient, BinanceSocketManager
import config
import pandas as pd
import json
import asyncpg

async def write_to_db(connection, res):
    await connection.execute("""INSERT INTO bitcoin_price(id,symbol,price) VALUES ($1,$2,$3)""",res['t'],res['s'],res['p'])
    print("printed",res['t'])
async def main():
    client = await AsyncClient.create(config.API_KEY,config.API_SECRET)
    #Create Database connection
    conn = await asyncpg.connect(host=config.DB_HOST,database=config.DB_NAME,user=config.DB_USER,password=config.DB_PASSWORD)


    #Create socket connection
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    ts = bm.trade_socket('BNBBTC')
    # then start receiving messages
    async with ts as tscm:
        while True:
            res = await tscm.recv()
            # print(res)
             
            keys = {'t','s','p'}
            lst = { key:value  for key,value in res.items() if key in keys}
            lst['p'] = float(lst['p'])
            print(lst)
            await write_to_db(conn,lst)

    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())