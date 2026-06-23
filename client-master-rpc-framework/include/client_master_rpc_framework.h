#pragma once

#include <chrono>
#include <cstdlib>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <boost/functional/hash.hpp>
#include <glog/logging.h>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/ibverbs/ib_socket.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "rpc_types.h"
#include "types.h"

namespace mooncake::client_master_rpc {

static const std::string kDefaultMasterAddress = "localhost:50051";

namespace detail {

template <typename Variant, typename T>
struct variant_contains : std::false_type {};

template <typename... Ts, typename T>
struct variant_contains<std::variant<Ts...>, T>
    : std::bool_constant<(std::is_same_v<Ts, T> || ...)> {};

template <typename Variant, typename T>
inline constexpr bool variant_contains_v =
    variant_contains<std::decay_t<Variant>, T>::value;

template <typename SocketConfigVariant>
inline void MaybeEnableRdmaSocketConfig(SocketConfigVariant& socket_config) {
    if constexpr (variant_contains_v<SocketConfigVariant,
                                     coro_io::ib_socket_t::config_t>) {
        socket_config = coro_io::ib_socket_t::config_t{};
    }
}

}  // namespace detail

// Shared RPC contract. Keep declarations in sync between the master process and
// clients. The concrete method bodies should be supplied by the application.
class WrappedMasterService {
   public:
    tl::expected<std::string, ErrorCode> ServiceReady();
    tl::expected<bool, ErrorCode> ExistKey(const std::string& key);
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key);
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config);
    tl::expected<void, ErrorCode> PutEnd(const UUID& client_id,
                                         const std::string& key,
                                         ReplicaType replica_type);
    tl::expected<void, ErrorCode> PutRevoke(const UUID& client_id,
                                            const std::string& key,
                                            ReplicaType replica_type);
    tl::expected<void, ErrorCode> Remove(const std::string& key,
                                         bool force = false);
    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);
    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);
    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        WrappedMasterService& wrapped_master_service);

template <auto Method>
struct RpcNameTraits;

#define MOONCAKE_RPC_NAME(method)                         \
    template <>                                           \
    struct RpcNameTraits<&WrappedMasterService::method> { \
        static constexpr const char* value = #method;     \
    }

MOONCAKE_RPC_NAME(ServiceReady);
MOONCAKE_RPC_NAME(ExistKey);
MOONCAKE_RPC_NAME(BatchExistKey);
MOONCAKE_RPC_NAME(GetReplicaList);
MOONCAKE_RPC_NAME(BatchGetReplicaList);
MOONCAKE_RPC_NAME(PutStart);
MOONCAKE_RPC_NAME(PutEnd);
MOONCAKE_RPC_NAME(PutRevoke);
MOONCAKE_RPC_NAME(Remove);
MOONCAKE_RPC_NAME(MountSegment);
MOONCAKE_RPC_NAME(UnmountSegment);
MOONCAKE_RPC_NAME(Ping);

#undef MOONCAKE_RPC_NAME

class MasterClientBase {
   public:
    explicit MasterClientBase(const UUID& client_id);
    ~MasterClientBase();

    MasterClientBase(const MasterClientBase&) = delete;
    MasterClientBase& operator=(const MasterClientBase&) = delete;

    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultMasterAddress);

   protected:
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args);

    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args);

    const UUID client_id_;

   private:
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool);
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool();

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

    RpcClientAccessor client_accessor_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    std::string client_addr_param_;
};

}  // namespace mooncake::client_master_rpc

namespace mooncake::client_master_rpc {

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClientBase::invoke_rpc(
    Args&&... args) {
    auto pool = client_accessor_.GetClientPool();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available for RPC "
                           << RpcNameTraits<ServiceMethod>::value;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC " << RpcNameTraits<ServiceMethod>::value
                           << " failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>>
MasterClientBase::invoke_batch_rpc(size_t input_size, Args&&... args) {
    auto pool = client_accessor_.GetClientPool();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                  std::vector<tl::expected<ResultType, ErrorCode>>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                std::vector<tl::expected<ResultType, ErrorCode>> error_results;
                error_results.reserve(input_size);
                for (size_t i = 0; i < input_size; ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
}

}  // namespace mooncake::client_master_rpc
