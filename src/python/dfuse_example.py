from __future__ import annotations

# Standard library includes
from typing import AsyncGenerator, Dict, Tuple, Iterable, Optional
import json
import asyncio
from pathlib import Path
import string
import time
from collections import OrderedDict
import base64
from pprint import pprint

# Library includes
import aiohttp
import grpc
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict
import abieos
from abieos import EosAbiSerializer

# Protobuf includes
from dfuse.eosio.codec.v1 import codec_pb2 as eos_codec_pb2
from graphql import graphql_pb2_grpc
from graphql import graphql_pb2
from dfuse.bstream.v1 import bstream_pb2
from dfuse.bstream.v1 import bstream_pb2_grpc


class Throttler:
  """
  Throttler for an API, make sure that no calls to an API happen within
  rate_limit_time seconds.
  """
  def __init__(self, *, rate_limit_time: float, t0: int):
    self.rate_limit_time = rate_limit_time
    # Timestamp of last throttle.
    self.t = t0

  async def throttle(self):
    t = time.time()

    self.t += self.rate_limit_time
    self.t = max(self.t, t)
    dt = self.t - t
    await asyncio.sleep(dt)


class DFuseGraphQLAPI:
  def __init__(self, *, authpoint: str, endpoint: str, api_key: str,
               throttler: Throttler):
    self._api_key = api_key
    self._auth: Optional[dict] = None
    self._authpoint = authpoint
    self._endpoint = endpoint
    self._client: Optional[graphql_pb2_grpc.GraphQLStub] = None
    self._throttler = throttler

  async def login(self):
    url = f'{self._authpoint}/v1/auth/issue'
    headers = {'Content-Type': 'application/json'}
    jsondata = {"api_key": self._api_key}

    async with aiohttp.ClientSession() as session:
      r = await session.post(url, headers=headers, json=jsondata)
      async with r:
        self._auth = await r.json()
        assert 'token' in self._auth, f'Could not auth, here is what was returned: {self._auth}, url: {url}, api_key: {self._api_key}'

    self._channel = grpc.aio.secure_channel(
        self._endpoint,
        credentials=grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.access_token_call_credentials(self._auth['token'])))

    self._client = graphql_pb2_grpc.GraphQLStub(self._channel)

  async def execute(self, *, query: str,
                    cursor: Optional[str]) -> AsyncGenerator[dict, None]:
    assert self._client is not None, "Must call login() first."

    variables = Struct()
    variables.update({'cursor': cursor})
    await self._throttler.throttle()
    stream = self._client.Execute(
        graphql_pb2.Request(query=query, variables=variables))

    async for rawResult in stream:
      if rawResult.errors:
        raise DFuseIOAPIError(rawResult.errors)
      else:
        item = json.loads(rawResult.data)
        yield item
        # TODO: throttle again?
        # await self._throttler.throttle()


async def load_abis_from_dfuse(
    *, accounts: Iterable[str],
    dfuse_graphql_api: DFuseGraphQLAPI) -> Dict[str, Dict[int, str]]:
  """
  Get all the the ABIs for each of the accounts in `accounts`.

  returns: {account => {global_sequence => ABI}}. ABI is in hex.
  """

  account2seq2abi = {}
  for account in accounts:
    # For each account.

    # Collection for all the APIs for this account:
    account2seq2abi[account] = OrderedDict()

    query = f'''
    query {{
      searchTransactionsForward(query:"receiver:eosio action:setabi data.account:{account}") {{
        cursor
        results {{
          trace {{
            matchingActions {{
              data
              receipt {{
                globalSequence
              }}
            }}
          }}
        }}
      }}
    }}
    '''
    cursor = None

    globalseq2abi = {}
    while True:
      found_something = False
      async for item in dfuse_graphql_api.execute(query=query, cursor=cursor):
        for result in item['searchTransactionsForward']['results']:
          for action in result['trace']['matchingActions']:
            abi = action['data']['abi']
            global_sequence = int(action['receipt']['globalSequence'])
            assert isinstance(global_sequence, int)
            assert isinstance(abi, str)
            if global_sequence in globalseq2abi:
              continue
            globalseq2abi[global_sequence] = abi
            found_something = True
        next_cursor = item['searchTransactionsForward']['cursor']
        # assert cursor != next_cursor, cursor
        cursor = next_cursor
      if not found_something:
        break
      await dfuse_graphql_api._throttler.throttle()
    for global_sequence in sorted(globalseq2abi.keys()):
      abi = globalseq2abi[global_sequence]
      account2seq2abi[account][global_sequence] = abi
  for account, seq2abi in account2seq2abi.items():
    for seq, abi in seq2abi.items():
      assert isinstance(seq, int)
  return account2seq2abi


async def load_abis_from_disk(
    *, abi_cache_path: str) -> Dict[str, Dict[int, str]]:
  with open(abi_cache_path, 'r') as f:
    account2seq2abi = json.load(fp=f)

    # json turned all the global_sequences to strings (because all keys are strings?)
    for account in account2seq2abi.keys():
      seq2abi = account2seq2abi[account]
      account2seq2abi[account] = dict([(int(seq), abi)
                                       for seq, abi in seq2abi.items()])
    return account2seq2abi


async def load_abis(*, accounts: Iterable[str],
                    dfuse_graphql_api: DFuseGraphQLAPI,
                    abi_cache_path: str) -> Dict[str, Dict[int, str]]:
  """
  Load ABI, either from cache file, or from dFuse GraphQL API.
  """
  class ReloadABIs(Exception):
    pass

  try:
    account2seq2abi = await load_abis_from_disk(abi_cache_path=abi_cache_path)
    for account in accounts:
      if account not in account2seq2abi:
        raise ReloadABIs()

    return account2seq2abi
  except (FileNotFoundError, json.decoder.JSONDecodeError):
    account2seq2abi = await load_abis_from_dfuse(
        accounts=accounts, dfuse_graphql_api=dfuse_graphql_api)
    with open(abi_cache_path, 'w') as f:
      json.dump(account2seq2abi, fp=f, indent=2)
    return account2seq2abi


class DFuseEOSFirehoseAPI:
  def __init__(self, api_key: str, authpoint: str, endpoint: str):
    self._api_key = api_key
    # Auth retrieved from server, via login().
    self._auth: Optional[dict] = None
    # Server to obtain auth from.
    self._authpoint = authpoint
    # Server to obtain the data from.
    self._endpoint = endpoint
    # gRPC client, obtained via login().
    self._client: Optional[Any] = None

  async def login(self):
    url = f'{self._authpoint}/v1/auth/issue'
    headers = {'Content-Type': 'application/json'}
    jsondata = {"api_key": self._api_key}

    async with aiohttp.ClientSession() as session:
      r = await session.post(url, headers=headers, json=jsondata)
      async with r:
        self._auth = await r.json()
        assert 'token' in self._auth, f'Could not auth, here is what was returned: {self._auth}, url: {url}, api_key: {self._api_key}'
    self._channel = grpc.aio.secure_channel(
        self._endpoint,
        credentials=grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.access_token_call_credentials(self._auth['token'])))

    self._client = bstream_pb2_grpc.BlockStreamV2Stub(self._channel)

  async def firehose(
      self, start_cursor: Optional[str], start_block_num: Optional[int],
      accounts: Optional[Iterable[str]],
      **kwargs) -> AsyncGenerator[Tuple[str, eos_codec_pb2.Block], None]:
    """
    Stream's blocks from firehose.

    * start_cursor: Specify this as the previously returned cursor value, or None.
    * start_block_num: The first block to stream.
    * accounts: Only return transactions that belong to these accounts.
    * kwargs: Other arguments, passed to bstream_pb2.BlocksRequestV2, which is documented here:
      https://github.com/dfuse-io/proto/blob/4bca1300d6e75a04724925ef2c9341dfe50c5263/dfuse/bstream/v1/bstream.proto#L48
    * returns: a tuple of (cursor, eos_codec_pb2.Block).
      * `cursor` can be used in followup calls to `firehose()` for `start_cursor`.
      * eos_codec_pb2.Block which is documented here:
        https://github.com/dfuse-io/proto-eosio/blob/6e8632d67a83918eb4cfc5e99ac51fc15c301b3a/dfuse/eosio/codec/v1/codec.proto#L9
    """

    assert self._client is not None, "Must call login() first"
    # Firehose takes an include_filter_expr, which only returns relevant
    # transactions.
    #
    # The language of this expression is CEL
    # (https://opensource.google/projects/cel).
    #
    # TODO: I figured out account by trial and error, but at what hierarchy of
    # the protos does this operate/filter on?
    #
    # Example:
    #
    # include_filter_expr = f'account=="X" || account=="Y" || account=="Z"'
    if accounts is not None:
      assert 'include_filter_expr' not in kwargs, "You can only specify one of `accounts` and `include_filter_expr`"
      kwargs['include_filter_expr'] = ' || '.join(
          [f'account=="{account}"' for account in accounts])
    # https://github.com/dfuse-io/proto/blob/4bca1300d6e75a04724925ef2c9341dfe50c5263/dfuse/bstream/v1/bstream.proto#L48
    request = bstream_pb2.BlocksRequestV2(start_cursor=start_cursor,
                                          start_block_num=start_block_num,
                                          **kwargs)

    # Call the `Blocks()`` method on the `bstream_pb2_grpc.BlockStreamV2Stub` service.
    #
    # Documented: https://github.com/dfuse-io/proto/blob/4bca1300d6e75a04724925ef2c9341dfe50c5263/dfuse/bstream/v1/bstream.proto#L15
    stream = self._client.Blocks(request)

    async for response in stream:
      # `response` is of type `bstream_pb2_grpc.BlockResponseV2`.
      #
      # See https://github.com/dfuse-io/proto/blob/4bca1300d6e75a04724925ef2c9341dfe50c5263/dfuse/bstream/v1/bstream.proto#L98

      # `response.block` is a `google.protobuf.Any`
      #
      # See https://github.com/dfuse-io/proto/blob/4bca1300d6e75a04724925ef2c9341dfe50c5263/dfuse/bstream/v1/bstream.proto#L102
      #
      # response.block is documented as holding either an EOS Block or an Etherium `Block`.
      #
      # This API is for EOS, so we use eos_codec_pb2.Block, documented here:
      # https://github.com/dfuse-io/proto-eosio/blob/6e8632d67a83918eb4cfc5e99ac51fc15c301b3a/dfuse/eosio/codec/v1/codec.proto#L9
      #
      # So, first create a default-initialized block.
      block = eos_codec_pb2.Block()
      # This is how you convert an Any to something specific in protobuf.
      response.block.Unpack(block)

      yield (response.cursor, block)

  @staticmethod
  def get_abi(*, account: str, global_sequence: int,
              account2seq2abi: Dict[str, Dict[int, str]]) -> Optional[bytes]:
    """
    Get ABI for an account at a particular action. If no ABI is available, return none.

    * global_sequence: The global sequence for this action.
    * account2seq2abi: Use load_abis() to obtain this.
    """
    if account not in account2seq2abi:
      return None

    assert isinstance(global_sequence, int)

    seq2abi = account2seq2abi[account]
    # Take the global sequences for the ABIs and sort them; we want to find the
    # last ABI that precedes `global_sequence`.
    seqs = list(sorted(seq2abi.keys()))

    # If there is no ABI, there is no ABI.
    if not seqs:
      return None

    def find_sequence():
      # For each ABI's global_sequence,
      for i, seq in enumerate(seqs):
        assert isinstance(seq, int)

        # If the ABI's `global_sequence` is after the requested global_sequence,
        if seq >= global_sequence:
          # Return the previous sequence.
          if i > 0:
            return seqs[i - 1]
          # There is no previous sequence, so there is no ABI.
          return None
      # If there is no ABI that is later than the requested `global_sequence`,
      # then just return the last ABI.
      return seqs[-1]

    # Find the ABI that precedes `global_sequence`.
    seq = find_sequence()

    if seq is None:
      # There is no mmatching ABI.
      return None

    # Sanity check.
    assert global_sequence >= seq

    # Get the corresponding ABI.
    abi = seq2abi[seq]

    # Sanity check.
    assert isinstance(abi, str) and set(abi) <= set(string.hexdigits), abi

    return abi

  @staticmethod
  def get_action_trace(*, db_op: dict,
                       filtered_transaction_trace: dict) -> dict:
    """
    Given a DBOp dictionary, get the corresponding action trace that generated
    it.
    """
    action_index = db_op['action_index']
    action_trace = filtered_transaction_trace['action_traces'][action_index]
    assert action_trace['execution_index'] == action_index
    return action_trace

  @staticmethod
  def parse_table_row(*, db_op: Optional[dict], action_trace: Optional[dict],
                      global_sequence: int, account: str, table_name: str,
                      data: Optional[str], account2seq2abi: Dict[str,
                                                                 Dict[int,
                                                                      str]],
                      serializer_cache: dict) -> Optional[dict]:
    """
    Parse the new_data or old_data sections (which are in base64, since
    protobuf's `google.protobuf.json_format.MessageToDict` converts bytes fields
    to base64.) into a dictionary of values.
    
    See https://github.com/dfuse-io/proto-eosio/blob/6e8632d67a83918eb4cfc5e99ac51fc15c301b3a/dfuse/eosio/codec/v1/codec.proto#L609

    * db_op: The db_op that generated this, used for debugging purposes.
    * action_trace: The action_trace that generated this, used for debugging
      purposes.
    * account: Contract account that owns the ABI that can interpret this data; 
      use db_op.code.
    * table_name: Table name, use db_op.table_name.
    * global_sequence: global_sequence of this action. Use 
      `int(action_trace.receipt.global_sequence)`.
    * data: new_data or old_data blog, encoded as base64 in a str.
    * returns: Dictionary of table row values, or None if `data` is the empty
      string or None.
    """

    assert isinstance(global_sequence, int)
    if data is None or data == '':
      # `google.protobuf.json_format.MessageToDict` translates null values to
      # the empty string.
      return None

    try:
      abi = DFuseEOSFirehoseAPI.get_abi(account=account,
                                        global_sequence=global_sequence,
                                        account2seq2abi=account2seq2abi)

      assert abi is not None, f"No ABI available for account={code}, global_sequence={global_sequence}"

      abi_serializer = DFuseEOSFirehoseAPI.get_serializer(
          contract='eosio.token', abi=abi, cache=serializer_cache)
      parsed_row = abi_serializer.bin_to_json(
          'eosio.token',
          abi_serializer.get_type_for_table('eosio.token', table_name),
          base64.b64decode(data))
      return parsed_row
    except Exception:
      # If there is an error, save all the arguments to a file so it can be
      # replayed.
      with open('parse_table_row_args.json', 'wb') as f:
        import pickle
        kwargs = {
            'db_op': db_op,
            'action_trace': action_trace,
            'account': account,
            'table_name': table_name,
            'global_sequence': global_sequence,
            'data': data,
            'account2seq2abi': account2seq2abi,
        }
        pickle.dump(kwargs, file=f)
      raise

  @staticmethod
  def get_serializer(*, contract: str, abi: str,
                     cache: dict) -> EosAbiSerializer:
    """
    This creates and caches `abieos.EosAbiSerializer` objects for each contract/abi.

    contract: Account name of the contract (db_op.code).

    abi: A blob encoded as hex in a string.

    cache: A dictionary used for caching.

    returns: Relevant `abieos.EosAbiSerializer`.
    """
    if (contract, abi) not in cache:
      abi_serializer = EosAbiSerializer()
      abi_serializer.set_abi_from_hex(contract, abi)
      cache[(contract, abi)] = abi_serializer
    return cache[(contract, abi)]

  async def parse_db_ops(
      self, *, block_dict: dict, account2seq2abi: Dict[str, Dict[int, str]],
      serializer_cache: Dict[Tuple[str, str],
                             str]) -> AsyncGenerator[dict, None]:
    """
    * block_dict: The block, converted to dictionary, using
      `google.protobuf.json_format.MessageToDict`.
    * account2seq2abi: Use `load_abis()` to get this.
    * serializer_cache: A cache for ABI serializers (used to decrypt blob of new_data and old_data to a table-row dictionary.).
    * returns: yields a dictionary corresponding to
      https://github.com/dfuse-io/proto-eosio/blob/6e8632d67a83918eb4cfc5e99ac51fc15c301b3a/dfuse/eosio/codec/v1/codec.proto#L600
      * The dictionary is enhanced with the corresponding `action_trace` in the
        `"action_trace"` field.
    """

    for filtered_transaction_trace in block_dict[
        'filtered_transaction_traces']:
      # For each filtered transaction.
      #
      # See https://github.com/dfuse-io/proto-eosio/blob/6e8632d67a83918eb4cfc5e99ac51fc15c301b3a/dfuse/eosio/codec/v1/codec.proto#L96
      #
      # Crawl the structure with your eyes to see how/why the following loops
      # are looped.

      for db_op in filtered_transaction_trace['db_ops']:
        # For each database operation in each transaction.

        # Shortcut to get the action_trace.
        action_trace = DFuseEOSFirehoseAPI.get_action_trace(
            db_op=db_op, filtered_transaction_trace=filtered_transaction_trace)
        if not action_trace['filtering_matched']:
          continue

        global_sequence = int(action_trace['receipt']['global_sequence'])
        # Code is used to determine which ABI contract to use.
        code = db_op['code']
        table_name = db_op['table_name']

        # Redefine new_data and old_data to be python dictionaries, instead of
        # encoded base-64 encoded blobs.
        #
        # These variables are of type `bytes` in the proto.
        #
        # `google.protobuf.json_format.MessageToDict` turns `bytes` into a
        # base64 encoded string.
        #
        # This function turns the blob into a dictionary by using abieos
        # library, and using the appropriate ABI.
        new_data = DFuseEOSFirehoseAPI.parse_table_row(
            db_op=db_op,
            action_trace=action_trace,
            account=code,
            table_name=table_name,
            global_sequence=global_sequence,
            data=db_op['new_data'],
            serializer_cache=serializer_cache,
            account2seq2abi=account2seq2abi)
        old_data = DFuseEOSFirehoseAPI.parse_table_row(
            db_op=db_op,
            action_trace=action_trace,
            account=code,
            table_name=table_name,
            global_sequence=global_sequence,
            data=db_op['old_data'],
            serializer_cache=serializer_cache,
            account2seq2abi=account2seq2abi)

        db_op['new_data'] = new_data
        db_op['old_data'] = old_data
        # Enhance the dictionary with the relevant action_trace.
        db_op['action_trace'] = action_trace
        yield db_op


################################################################################
# Example usage below.
################################################################################


def load_cursor_from_disk():
  with open(f'cursor', 'r') as f:
    return f.read()


def load_cursor():
  """
  Load cached cursor from disk or None.
  """
  try:
    cursor = load_cursor_from_disk()
    if cursor is None or len(cursor) == 0:
      return None
    return cursor
  except FileNotFoundError:
    return None


async def async_main():
  import argparse
  parser = argparse.ArgumentParser(description='dFuse aio gRPC example.')
  parser.add_argument('--firehose_authpoint',
                      type=str,
                      required=True,
                      help="Address to the dFuse Firehose auth server")
  parser.add_argument('--firehose_endpoint',
                      type=str,
                      required=True,
                      help="Address to the dFuse Firehose server")
  parser.add_argument('--firehose_api_key',
                      type=str,
                      required=True,
                      help="API key for the Firehose server.")
  parser.add_argument('--graphql_authpoint',
                      type=str,
                      required=True,
                      help="Address to the dFuse GraphQL auth server")
  parser.add_argument('--graphql_endpoint',
                      type=str,
                      required=True,
                      help="Address to the dFuse GraphQL server")
  parser.add_argument('--graphql_api_key',
                      type=str,
                      required=True,
                      help="API key for the dFuse GraphQL server.")
  parser.add_argument('--start_block_num',
                      type=int,
                      required=True,
                      help="Which block to start from.")
  parser.add_argument(
      '--accounts',
      type=str,
      required=True,
      help=
      "Accounts we are interested in, comma separated. Will filter the firehose by these accounts."
  )
  parser.add_argument(
      '--cache_path',
      type=str,
      default="cache",
      help="Path to a cache directory for various caching of stuff.")
  args = parser.parse_args()

  # Turn account,account,account into ['account','account','account',].
  accounts = args.accounts.split(',')

  # We need the GraphQL API to obtain the relevant ABIs
  dfuse_graphql_api = DFuseGraphQLAPI(api_key=args.graphql_api_key,
                                      authpoint=args.graphql_authpoint,
                                      endpoint=args.graphql_endpoint,
                                      throttler=Throttler(rate_limit_time=1.1,
                                                          t0=time.time()))

  dfuse_firehose_api = DFuseEOSFirehoseAPI(api_key=args.firehose_api_key,
                                           authpoint=args.firehose_authpoint,
                                           endpoint=args.firehose_endpoint)

  await dfuse_graphql_api.login()
  await dfuse_firehose_api.login()

  Path(args.cache_path).mkdir(parents=True, exist_ok=True)
  # Create output directory for blocks, if it doesn't exist.
  Path.cwd().joinpath('blocks/').mkdir(parents=True, exist_ok=True)
  # Create output directory for db_ops, if it doesn't exist.
  Path.cwd().joinpath('db_ops/').mkdir(parents=True, exist_ok=True)

  # This maps (account, action_global_sequence) => ABI (in hex).
  #
  # ABI is used to interpret the data for each db_op's diff in the tables.
  account2seq2abi = await load_abis(
      accounts=accounts,
      dfuse_graphql_api=dfuse_graphql_api,
      abi_cache_path=str(
          Path(args.cache_path).joinpath('account2seq2abi.json')))

  # This is going to be used to cache abieos.EosAbiSerializer objects.
  serializer_cache = {}

  # Load last cursor from a file if available (None otherwise).
  cursor = load_cursor()
  while True:
    # Drink from the firehose.
    async for (cursor, block) in dfuse_firehose_api.firehose(
        start_cursor=cursor,
        start_block_num=args.start_block_num,
        accounts=accounts,
        # Only get irreversible blocks.
        fork_steps=['STEP_IRREVERSIBLE']):
      # Convert the block to a dictionary.
      block_dict = MessageToDict(block,
                                 preserving_proto_field_name=True,
                                 including_default_value_fields=True)

      # Track which db_op we are in.
      db_op_index = 0
      # For each db_op in the block.
      async for db_op in dfuse_firehose_api.parse_db_ops(
          block_dict=block_dict,
          account2seq2abi=account2seq2abi,
          serializer_cache=serializer_cache):
        with open(f'db_ops/{block.id}-{db_op_index}.json', 'w') as f:
          # Save the block to a file.
          json.dump(block_dict, fp=f, indent=2)

        # Print the old data and new data for the row.
        print('table_name:', db_op['table_name'])
        print('old_data:')
        pprint(db_op['old_data'])
        print('new_data:')
        pprint(db_op['new_data'])

        db_op_index += 1

      # Ensure the block id is sane, befure using it in a path
      assert set(block.id) <= set(string.hexdigits)

      with open(f'blocks/{block.id}.json', 'w') as f:
        # Save the block to a file.
        json.dump(block_dict, fp=f, indent=2)

      with open(str(Path(args.cache_path).joinpath('cursor')), 'w') as f:
        # Save the cursor to a file.
        f.write(cursor)


if __name__ == '__main__':
  # Uncomment this to test parse_table_row with pickled arguments.
  # import pickle
  # with open('parse_table_row_args.json', 'rb') as f:
  #   kwargs = pickle.load(file=f)
  #   serializer_cache = {}
  #   DFuseEOSFirehoseAPI.parse_table_row(serializer_cache=serializer_cache, **kwargs)

  exit(asyncio.get_event_loop().run_until_complete(async_main()))
