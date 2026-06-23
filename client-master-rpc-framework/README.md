# Client/Master RPC Registration Framework

This directory extracts the Mooncake Store client/master registration and
invocation framework into a small, standalone reference package. It is intended
as a readable starting point for reusing the communication skeleton without the
full store implementation.

## What is included

- `include/client_master_rpc_framework.h`
  - RPC service method declarations used as the shared client/server contract.
  - `ClientMemoryRegistration` records the host ID, reserved-memory base
    address, and size that a client reports when it starts.
  - `MasterMemoryRegistry` stores registered client memory as a placeholder for
    the master's future global allocator.
  - `RegisterRpcService(...)`, the server-side handler registration entrypoint.
  - `MasterClientBase`, the client-side connection/pool and generic RPC invoke
    helpers, including initialization-time memory registration.
- `src/client_master_rpc_framework.cpp`
  - Server-side `coro_rpc_server::register_handler` calls.
  - Client-side pool selection, `ServiceReady` connection check, and generic
    `send_request` wrappers.
  - Client memory registration through `RegisterClientMemory`.

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

## Client memory registration flow

1. A client reserves a local memory range during startup.
2. The client calls `InitializeAndRegisterMemory(master_addr, registration)`.
3. `MasterClientBase` connects to the master, checks `ServiceReady`, and sends
   `RegisterClientMemory`.
4. The master-side service records `{host_id, base_addr, size}` in
   `MasterMemoryRegistry`.
5. TODO: replace the registry map with the master's unified memory allocator so
   subsequent object placement can allocate subranges from registered client
   memory.
