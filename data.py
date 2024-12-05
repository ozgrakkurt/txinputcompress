import hypersync
from hypersync import TransactionField, ClientConfig, Query, TransactionSelection, FieldSelection, StreamConfig
import asyncio

DATA_FILE_PATH = "input.data"
INDEX_FILE_PATH = "input.index"

SMALL_DATA_FILE_PATH = "small_input.data"
SMALL_INDEX_FILE_PATH = "small_input.index"

LARGE_DATA_FILE_PATH = "large_input.data"
LARGE_INDEX_FILE_PATH = "large_input.index"

INPUT_SIZE_TRESHOLD = 32 * 1024 

TOTAL_TX = 1000000

async def main():
    client = hypersync.HypersyncClient(ClientConfig())
    query = Query(
        from_block=18123123,
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

    small_data_file = open(SMALL_DATA_FILE_PATH, 'wb')
    small_index_file = open(SMALL_INDEX_FILE_PATH, 'w')

    large_data_file = open(LARGE_DATA_FILE_PATH, 'wb')
    large_index_file = open(LARGE_INDEX_FILE_PATH, 'w')

    num_tx = 0
    offset = 0
    large_offset = 0
    small_offset = 0

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

                if len(buf) >= INPUT_SIZE_TRESHOLD:
                    large_offset += len(buf)
                    large_data_file.write(buf)
                else:
                    small_offset += len(buf)
                    small_data_file.write(buf)

                large_index_file.write(' ' + str(large_offset))
                small_index_file.write(' ' + str(small_offset))

        print("processed up to block " + str(res.next_block) + "\n")

        if num_tx >= TOTAL_TX:
            break

    data_file.close()
    index_file.close()

    small_data_file.close()
    small_index_file.close()

    large_data_file.close()
    large_index_file.close()

asyncio.run(main())
