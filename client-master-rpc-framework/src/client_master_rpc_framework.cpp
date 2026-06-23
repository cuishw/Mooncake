#include "client_master_rpc_framework.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>

namespace mooncake::client_master_rpc {

tl::expected<void, ErrorCode> MasterMemoryRegistry::RegisterClientMemory(
    const ClientMemoryRegistration& registration) {
    if (registration.size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    registered_memory_[registration.host_id] = registration;
    return {};
}

std::unordered_map<UUID, ClientMemoryRegistration, boost::hash<UUID>>
MasterMemoryRegistry::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return registered_memory_;
}

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        WrappedMasterService& wrapped_master_service) {
    server.register_handler<&WrappedMasterService::ServiceReady>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::GetReplicaList>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::BatchGetReplicaList>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::PutStart>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::Ping>(
        &wrapped_master_service);
    server.register_handler<&WrappedMasterService::RegisterClientMemory>(
        &wrapped_master_service);
}

MasterClientBase::MasterClientBase(const UUID& client_id)
    : client_id_(client_id) {
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};

    // Disable alive detection so stale pools do not keep probing failed master
    // addresses after failover/reconfiguration.
    pool_conf.host_alive_detect_duration = std::chrono::seconds(0);

    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    if (protocol && std::string_view(protocol) == "rdma") {
        detail::MaybeEnableRdmaSocketConfig(
            pool_conf.client_config.socket_config);
    }

    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
}

MasterClientBase::~MasterClientBase() = default;

ErrorCode MasterClientBase::Connect(const std::string& master_addr) {
    if (client_addr_param_ != master_addr) {
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
    }

    auto result =
        invoke_rpc<&WrappedMasterService::ServiceReady, std::string>();
    if (!result.has_value()) {
        return result.error();
    }
    return ErrorCode::OK;
}

ErrorCode MasterClientBase::InitializeAndRegisterMemory(
    const std::string& master_addr,
    const ClientMemoryRegistration& registration) {
    auto connect_result = Connect(master_addr);
    if (connect_result != ErrorCode::OK) {
        return connect_result;
    }
    return RegisterReservedMemory(registration);
}

ErrorCode MasterClientBase::RegisterReservedMemory(
    const ClientMemoryRegistration& registration) {
    auto result = invoke_rpc<&WrappedMasterService::RegisterClientMemory, void>(
        registration);
    if (!result.has_value()) {
        return result.error();
    }
    return ErrorCode::OK;
}

void MasterClientBase::RpcClientAccessor::SetClientPool(
    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        client_pool) {
    std::lock_guard<std::shared_mutex> lock(client_mutex_);
    client_pool_ = client_pool;
}

std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
MasterClientBase::RpcClientAccessor::GetClientPool() {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    return client_pool_;
}

}  // namespace mooncake::client_master_rpc
