

## Prerequisites

* bash, unix-like environment.
* python with asyncio at least.
* docker (for protoc).
* git (to get proto files).

## Running the example

1. Start a virtual environment.

  ```bash
  virtualenv .venv
  ```
2. Enter the virtual environment.

  ```bash
  source .venv/bin/activate
  ```
3. Install requirements.

  ```bash
  pip install -r requirements.txt
  ```
4. Generate protobuf python files.

  ```bash
  bash scripts/generate-protos.sh
  ```
5. Run the example.

  * Note that the example will create a directory `$PWD/cache`, which you should delete if you want to reset the example.
  * Note that `$PWD/blocks/` and `$PWD/db_ops/` will grow extremely large with all the data.
  
  ```bash
  FIREHOSE_AUTHPOINT=...
  FIREHOSE_ENDPOINT=...
  FIREHOSE_API_KEY=...
  GRAPHQL_AUTHPOINT=...
  GRAPHQL_ENDPOINT=...
  GRAPHQL_API_KEY=...
  PYTHONPATH=$PWD/src/python python -m dfuse_example \
    --firehose_authpoint "$FIREHOSE_AUTHPOINT" \
    --firehose_endpoint "$FIREHOSE_ENDPOINT" \
    --firehose_api_key "$FIREHOSE_API_KEY" \
    --graphql_authpoint "$GRAPHQL_AUTHPOINT" \
    --graphql_endpoint "$GRAPHQL_ENDPOINT" \
    --graphql_api_key "$GRAPHQL_API_KEY" \
    --start_block_num 170843515 \
    --accounts "playuplandme,upxtokenacct,communityupx"
  ```

## Other things

* `bash scripts/format.sh` to format