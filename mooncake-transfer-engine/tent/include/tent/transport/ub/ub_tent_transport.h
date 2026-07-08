// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef UB_TENT_TRANSPORT_H
#define UB_TENT_TRANSPORT_H

#include "tent/runtime/transport.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/topology.h"
#include "tent/common/types.h"
#include "tent/common/config.h"
#include "tent/common/status.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {
class UbTransport;

namespace tent {

// UbTentTransport is the TENT-native UB backend. It owns native UB tasks and
// slices, consumes TENT SegmentDesc metadata directly, and never submits work
// through the old adapter executor.
//
// Threading: install/uninstall are single-threaded; all other methods may be
// called concurrently from multiple TENT worker threads.
class UbTentTransport : public Transport {
   public:
    struct UbRemoteMetadata {
        std::string peer_nic_path;
        std::string remote_tseg;
        std::string remote_eid;
        std::vector<uint32_t> remote_jetty_num;
        int remote_device_id{-1};
        size_t target_buffer_index{0};
    };

    struct UbSlice {
        Request::OpCode opcode{Request::READ};
        void* source_addr{nullptr};
        uint64_t target_addr{0};
        size_t length{0};
        SegmentID target_id{LOCAL_SEGMENT_ID};
        UbRemoteMetadata remote;
        int priority{PRIO_HIGH};
        int selected_device_id{-1};
        TransferStatusEnum status{TransferStatusEnum::INITIAL};
        size_t transferred_bytes{0};
        uint64_t enqueue_ns{0};
        uint64_t submit_ns{0};
        uint64_t complete_ns{0};
        int retry_count{0};
    };

    struct UbTask {
        Request request{};
        std::vector<UbSlice> slices;
        TransferStatusEnum status{TransferStatusEnum::INITIAL};
        size_t transferred_bytes{0};
        mutable std::mutex mutex;
    };

    struct UbSubBatch : public Transport::SubBatch {
        std::vector<std::unique_ptr<UbTask>> task_list;
        size_t max_size{0};
        size_t size() const override { return task_list.size(); }
    };

    struct UbDeviceState {
        std::string name;
        int numa_node{0};
        std::atomic<uint64_t> inflight_bytes{0};
        std::atomic<uint64_t> completed_bytes{0};
        std::atomic<uint64_t> failed_count{0};
        std::atomic<double> ewma_bandwidth_bps{50e9};
        std::atomic<uint64_t> last_error_ns{0};
        std::atomic<uint64_t> cooldown_until_ns{0};
    };

    UbTentTransport() = default;
    ~UbTentTransport() override;

    // TENT Transport interface
    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;
    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "ub"; }

   private:
    struct LocalBufferRegistration {
        uint64_t addr{0};
        size_t length{0};
        std::vector<std::string> tseg;
        std::vector<uint32_t> l_seg_index;
    };

    size_t sliceSize() const;
    Status setupUbLocalSegment();
    Status resolveRemoteMetadata(const Request& req, size_t slice_offset,
                                 size_t length, int preferred_device_id,
                                 UbRemoteMetadata& remote);
    Status findLocalSegment(uint64_t addr, size_t length, int device_id,
                            uint32_t& l_seg_index);
    Status chooseDevice(uint64_t device_mask, size_t length, int priority,
                        int& device_id);
    Status submitRemoteSlice(UbTask& task, UbSlice& slice);
    void completeSlice(UbSlice& slice, TransferStatusEnum status);
    void aggregateTask(UbTask& task);
    uint64_t currentTimeNs() const;

   private:
    bool installed_{false};
    std::shared_ptr<Config> conf_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> control_service_;
    std::string local_segment_name_;
    std::unique_ptr<mooncake::UbTransport> ub_primitive_;
    std::vector<std::unique_ptr<UbDeviceState>> devices_;
    std::mutex device_mutex_;
    std::vector<LocalBufferRegistration> registered_buffers_;
    std::mutex registered_buffers_mutex_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // UB_TENT_TRANSPORT_H
