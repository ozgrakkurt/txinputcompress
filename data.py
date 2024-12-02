import hypersync
from hypersync import TransactionField, ClientConfig, Query, TransactionSelection, FieldSelection, StreamConfig
import asyncio

DATA_FILE_PATH = "input.data"
INDEX_FILE_PATH = "input.index"
TOTAL_TX = 500000

async def main():
    client = hypersync.HypersyncClient(ClientConfig())
    query = Query(
        from_block=16123123,
        transactions=[TransactionSelection()],
        field_selection=FieldSelection(
            transaction=[
                TransactionField.INPUT,
            ]
        ),
    )

    receiver = await client.stream_arrow(query, StreamConfig())

    data_file = open(DATA_FILE_PATH, 'wb')
    index_file = open(INDEX_FILE_PATH, 'w')

    num_tx = 0
    offset = 0

    while True:
        res = await receiver.recv()

        if res is None:
            break

        for input in res.data.transactions.column('input'):
            num_tx += 1
            if input is not None and input.is_valid:
                buf = input.as_buffer()
                offset += len(buf)
                data_file.write(buf)
                index_file.write(' ' + str(offset))

        print("processed up to block " + str(res.next_block) + "\n")

        if num_tx >= TOTAL_TX:
            break

    data_file.close()
    index_file.close()

asyncio.run(main())
