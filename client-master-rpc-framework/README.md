# Client/Master RPC Registration Framework

This directory extracts the Mooncake Store client/master registration and
invocation framework into a small, standalone reference package. It is intended
as a readable starting point for reusing the communication skeleton without the
full store implementation.

## What is included

- `include/client_master_rpc_framework.h`
  - RPC service method declarations used as the shared client/server contract.
  - `RegisterRpcService(...)`, the server-side handler registration entrypoint.
  - `MasterClientBase`, the client-side connection/pool and generic RPC invoke
    helpers.
- `src/client_master_rpc_framework.cpp`
  - Server-side `coro_rpc_server::register_handler` calls.
  - Client-side pool selection, `ServiceReady` connection check, and generic
    `send_request` wrappers.

## Original source locations

The extracted framework mirrors the communication plumbing from:

- `mooncake-store/include/rpc_service.h`
- `mooncake-store/src/rpc_service.cpp`
- `mooncake-store/include/master_client.h`
- `mooncake-store/src/master_client.cpp`

Business logic implemented by `WrappedMasterService` remains in the original
Mooncake Store service files. To reuse this skeleton, provide concrete method
implementations for the declarations in `WrappedMasterService` and add/remove
registered methods as needed.
