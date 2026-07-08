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

#include "transport/kunpeng_transport/ub_transport.h"

#include "tent/transport/ub/ub_tent_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <sstream>

#include "tent/runtime/segment_manager.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

using json = nlohmann::json;

namespace {

constexpr size_t kDefaultUbSliceSize = 64 * 1024;
constexpr double kDefaultUbBandwidthBps = 50e9;

std::vector<std::string> parseDeviceFilter(const std::string& csv) {
    std::vector<std::string> out;
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        size_t b = item.find_first_not_of(" \t");
        if (b == std::string::npos) continue;
        size_t e = item.find_last_not_of(" \t");
        out.push_back(item.substr(b, e - b + 1));
    }
    return out;
}

json makeUbBufferAttrs(const std::vector<std::string>& tseg,
                       const std::vector<uint32_t>& l_seg_index) {
    return json{{"version", 1},
                {"buffers", {{"tseg", tseg}, {"l_seg_index", l_seg_index}}}};
}

std::vector<std::string> parseTsegList(const std::string& attrs) {
    if (attrs.empty()) return {};
    std::vector<std::string> out;
    auto j = json::parse(attrs);
    if (j.is_array()) {
        for (const auto& v : j) out.push_back(v.get<std::string>());
        return out;
    }
    if (j.is_object() && j.contains("buffers") &&
        j["buffers"].contains("tseg")) {
        for (const auto& v : j["buffers"]["tseg"]) {
            out.push_back(v.get<std::string>());
        }
    }
    return out;
}

std::string parseDeviceEid(const std::string& attrs) {
    if (attrs.empty()) return {};
    try {
        auto j = json::parse(attrs);
        if (j.is_object() && j.contains("eid"))
            return j["eid"].get<std::string>();
    } catch (const std::exception&) {
    }
    return attrs;
}

std::vector<uint32_t> parseDeviceJettyNum(const std::string& attrs) {
    std::vector<uint32_t> out;
    if (attrs.empty()) return out;
    try {
        auto j = json::parse(attrs);
        if (j.is_object() && j.contains("jetty_num")) {
            for (const auto& value : j["jetty_num"]) {
                out.push_back(value.get<uint32_t>());
            }
        }
    } catch (const std::exception&) {
    }
    return out;
}

bool containsTransport(const std::vector<TransportType>& transports,
                       TransportType type) {
    return std::find(transports.begin(), transports.end(), type) !=
           transports.end();
}

}  // namespace

UbTentTransport::~UbTentTransport() { uninstall(); }

Status UbTentTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    local_segment_name_ = local_segment_name;
    control_service_ = std::move(metadata);
    conf_ = std::move(conf);
    local_topology_ = local_topology ? std::move(local_topology)
                                     : std::make_shared<Topology>();

    std::string device_csv;
    if (conf_) {
        device_csv =
            conf_->get<std::string>("transports/ub/device_name", std::string());
    }
    if (device_csv.empty()) {
        if (const char* env = std::getenv("MC_UB_DEVICE_NAME")) {
            device_csv = env;
        }
    }

    std::vector<std::string> device_names = parseDeviceFilter(device_csv);
    std::vector<std::string> topology_device_names;
    if (device_names.empty() && local_topology_) {
        for (size_t i = 0; i < local_topology_->getNicCount(); ++i) {
            const auto* nic = local_topology_->getNicEntry(static_cast<int>(i));
            if (!nic) continue;
            if (nic->type == Topology::NIC_RDMA ||
                nic->type == Topology::NIC_UNKNOWN) {
                topology_device_names.push_back(
                    nic->name.empty() ? "ub" + std::to_string(i) : nic->name);
            }
        }
    }
    if (device_names.empty()) device_names = topology_device_names;

    auto te_topology = std::make_shared<mooncake::Topology>();
    if (!device_names.empty()) {
        te_topology->discover(device_names);
    }

    ub_primitive_ = std::make_unique<mooncake::UbTransport>();
    int ret = ub_primitive_->installPrimitive(local_segment_name_, te_topology);
    if (ret) {
        ub_primitive_.reset();
        return Status::DeviceNotFound(
            "UbTentTransport: cannot initialize native UB primitive" LOC_MARK);
    }
    auto native_devices = ub_primitive_->nativeDevices();
    if (native_devices.empty()) {
        ub_primitive_.reset();
        return Status::DeviceNotFound(
            "UbTentTransport: no native UB primitive devices" LOC_MARK);
    }
    {
        std::lock_guard<std::mutex> lock(device_mutex_);
        devices_.clear();
        for (size_t i = 0; i < native_devices.size(); ++i) {
            auto state = std::make_unique<UbDeviceState>();
            state->name = native_devices[i].name;
            state->ewma_bandwidth_bps.store(kDefaultUbBandwidthBps,
                                            std::memory_order_relaxed);
            if (local_topology_) {
                int nic_id = local_topology_->getNicId(state->name);
                if (const auto* nic = local_topology_->getNicEntry(nic_id)) {
                    state->numa_node = nic->numa_node;
                }
            }
            devices_.push_back(std::move(state));
        }
    }

    caps.dram_to_dram = true;
    installed_ = true;

    auto s = setupUbLocalSegment();
    if (!s.ok()) {
        LOG(WARNING) << "UbTentTransport: setup local segment failed: "
                     << s.message();
    }

    LOG(INFO) << "UbTentTransport: installed native UB transport on segment '"
              << local_segment_name_ << "' devices=" << devices_.size();
    return Status::OK();
}

Status UbTentTransport::uninstall() {
    installed_ = false;
    if (ub_primitive_) {
        ub_primitive_->uninstallPrimitive();
        ub_primitive_.reset();
    }
    {
        std::lock_guard<std::mutex> lock(device_mutex_);
        devices_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        registered_buffers_.clear();
    }
    control_service_.reset();
    local_topology_.reset();
    conf_.reset();
    return Status::OK();
}

Status UbTentTransport::setupUbLocalSegment() {
    if (!control_service_) return Status::OK();

    auto& manager = control_service_->segmentManager();
    auto segment = manager.getLocal();
    if (!segment) return Status::OK();
    if (segment->type != SegmentType::Memory) {
        return Status::InvalidArgument(
            "UbTentTransport: local segment is not memory" LOC_MARK);
    }

    auto& detail = std::get<MemorySegmentDesc>(segment->detail);
    auto native_devices =
        ub_primitive_ ? ub_primitive_->nativeDevices()
                      : std::vector<mooncake::UbTransport::NativeDeviceDesc>();
    {
        std::lock_guard<std::mutex> lock(device_mutex_);
        for (size_t i = 0; i < devices_.size(); ++i) {
            auto& dev = devices_[i];
            DeviceDesc* existing = nullptr;
            for (auto& d : detail.devices) {
                if (d.name == dev->name) {
                    existing = &d;
                    break;
                }
            }
            if (!existing) {
                DeviceDesc d;
                d.name = dev->name;
                detail.devices.push_back(std::move(d));
                existing = &detail.devices.back();
            }
            existing->transport_attrs[TransportType::UB] = json{
                {"version", 1},
                {"eid", i < native_devices.size() ? native_devices[i].eid
                                                  : std::string()},
                {"jetty_num", i < native_devices.size()
                                  ? native_devices[i].jetty_num
                                  : std::vector<uint32_t>()},
                {"device_id", i}}.dump();
        }
    }
    detail.transport_attrs[static_cast<int>(TransportType::UB)] =
        json{{"version", 1}, {"native", true}}.dump();
    return manager.synchronizeLocal();
}

Status UbTentTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    if (!installed_) {
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
    }

    if (!ub_primitive_) {
        return Status::InternalError(
            "UbTentTransport: native UB primitive is not installed" LOC_MARK);
    }

    mooncake::UbTransport::NativeMemoryDesc native_desc;
    int rc = ub_primitive_->registerLocalMemoryNative(
        reinterpret_cast<void*>(desc.addr), desc.length, native_desc);
    if (rc) {
        return Status::AddressNotRegistered(
            "UbTentTransport: native UB memory registration failed" LOC_MARK);
    }

    desc.location =
        options.location.empty() ? kWildcardLocation : options.location;
    desc.transport_attrs[TransportType::UB] =
        makeUbBufferAttrs(native_desc.tseg, native_desc.l_seg_index).dump();
    if (!containsTransport(desc.transports, TransportType::UB)) {
        desc.transports.push_back(TransportType::UB);
    }
    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        registered_buffers_.push_back(LocalBufferRegistration{
            desc.addr, desc.length, native_desc.tseg, native_desc.l_seg_index});
    }
    return Status::OK();
}

Status UbTentTransport::removeMemoryBuffer(BufferDesc& desc) {
    if (ub_primitive_) {
        ub_primitive_->unregisterLocalMemoryNative(
            reinterpret_cast<void*>(desc.addr));
    }
    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        registered_buffers_.erase(
            std::remove_if(registered_buffers_.begin(),
                           registered_buffers_.end(),
                           [&](const LocalBufferRegistration& entry) {
                               return entry.addr == desc.addr;
                           }),
            registered_buffers_.end());
    }
    auto& ts = desc.transports;
    ts.erase(std::remove(ts.begin(), ts.end(), TransportType::UB), ts.end());
    desc.transport_attrs.erase(TransportType::UB);
    return Status::OK();
}

Status UbTentTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    if (!installed_) {
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
    }
    auto* ub_batch = new UbSubBatch();
    ub_batch->max_size = max_size;
    ub_batch->task_list.reserve(max_size);
    batch = ub_batch;
    return Status::OK();
}

Status UbTentTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

size_t UbTentTransport::sliceSize() const {
    if (!conf_) return kDefaultUbSliceSize;
    size_t value = conf_->get<size_t>("transports/ub/workers/block_size", 0);
    if (value == 0) value = conf_->get<size_t>("transports/ub/slice_size", 0);
    if (value == 0) {
        value = conf_->get<size_t>("transports/rdma/workers/block_size",
                                   kDefaultUbSliceSize);
    }
    return value == 0 ? kDefaultUbSliceSize : value;
}

uint64_t UbTentTransport::currentTimeNs() const {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

Status UbTentTransport::chooseDevice(uint64_t device_mask, size_t length,
                                     int /*priority*/, int& device_id) {
    std::lock_guard<std::mutex> lock(device_mutex_);
    if (devices_.empty()) return Status::DeviceNotFound("no UB devices");

    double best_score = std::numeric_limits<double>::max();
    int best = -1;
    for (size_t i = 0; i < devices_.size(); ++i) {
        if ((device_mask & (1ULL << i)) == 0) continue;
        auto& dev = devices_[i];
        uint64_t now = currentTimeNs();
        if (dev->cooldown_until_ns.load(std::memory_order_relaxed) > now) {
            continue;
        }
        double bw = dev->ewma_bandwidth_bps.load(std::memory_order_relaxed);
        if (bw <= 0.0) bw = kDefaultUbBandwidthBps;
        double inflight = static_cast<double>(
            dev->inflight_bytes.load(std::memory_order_relaxed));
        double score = (inflight + static_cast<double>(length)) / bw;
        if (score < best_score) {
            best_score = score;
            best = static_cast<int>(i);
        }
    }
    if (best < 0) {
        return Status::DeviceNotFound("no UB devices allowed by device_mask");
    }
    devices_[best]->inflight_bytes.fetch_add(length, std::memory_order_relaxed);
    device_id = best;
    return Status::OK();
}

Status UbTentTransport::resolveRemoteMetadata(const Request& req,
                                              size_t slice_offset,
                                              size_t length,
                                              int preferred_device_id,
                                              UbRemoteMetadata& remote) {
    if (req.target_id == LOCAL_SEGMENT_ID) {
        remote.remote_device_id = 0;
        remote.peer_nic_path = local_segment_name_ + "@local";
        remote.remote_tseg = "local";
        remote.remote_eid = "local";
        remote.target_buffer_index = 0;
        return Status::OK();
    }
    if (!control_service_) {
        return Status::InvalidArgument(
            "UbTentTransport: remote UB transfer requires "
            "ControlService" LOC_MARK);
    }

    SegmentDesc* target = nullptr;
    auto status = control_service_->segmentManager().getRemoteCached(
        target, req.target_id);
    if (!status.ok() || !target) return status;
    if (target->type != SegmentType::Memory) {
        return Status::InvalidArgument(
            "UbTentTransport: target is not memory segment" LOC_MARK);
    }

    uint64_t addr = req.target_offset + slice_offset;
    auto& mem = std::get<MemorySegmentDesc>(target->detail);
    for (size_t i = 0; i < mem.buffers.size(); ++i) {
        const auto& buf = mem.buffers[i];
        if (addr < buf.addr || length > buf.length ||
            addr - buf.addr > buf.length - length) {
            continue;
        }
        auto it = buf.transport_attrs.find(TransportType::UB);
        if (it == buf.transport_attrs.end()) {
            return Status::InvalidArgument(
                "UbTentTransport: target buffer lacks UB attrs" LOC_MARK);
        }
        auto tseg = parseTsegList(it->second);
        if (tseg.empty()) {
            return Status::InvalidArgument(
                "UbTentTransport: target buffer has empty UB tseg" LOC_MARK);
        }

        size_t remote_device_index = 0;
        if (preferred_device_id >= 0 &&
            static_cast<size_t>(preferred_device_id) < tseg.size()) {
            remote_device_index = static_cast<size_t>(preferred_device_id);
        }
        remote.target_buffer_index = i;
        remote.remote_device_id = static_cast<int>(remote_device_index);
        remote.remote_tseg = tseg[remote_device_index];
        if (!mem.devices.empty()) {
            const auto& dev = mem.devices[std::min(remote_device_index,
                                                   mem.devices.size() - 1)];
            remote.peer_nic_path = target->nicPathServerName() + "@" + dev.name;
            auto dit = dev.transport_attrs.find(TransportType::UB);
            if (dit != dev.transport_attrs.end()) {
                remote.remote_eid = parseDeviceEid(dit->second);
                remote.remote_jetty_num = parseDeviceJettyNum(dit->second);
            }
        }
        return Status::OK();
    }

    return Status::InvalidArgument(
        "UbTentTransport: target address not in registered buffer" LOC_MARK);
}

Status UbTentTransport::findLocalSegment(uint64_t addr, size_t length,
                                         int device_id, uint32_t& l_seg_index) {
    std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
    for (const auto& buffer : registered_buffers_) {
        if (addr < buffer.addr || length > buffer.length ||
            addr - buffer.addr > buffer.length - length) {
            continue;
        }
        if (device_id < 0 ||
            static_cast<size_t>(device_id) >= buffer.l_seg_index.size()) {
            return Status::DeviceNotFound(
                "UbTentTransport: selected UB device is not "
                "registered" LOC_MARK);
        }
        l_seg_index = buffer.l_seg_index[device_id];
        return Status::OK();
    }
    return Status::AddressNotRegistered(
        "UbTentTransport: local source buffer is not registered for "
        "UB" LOC_MARK);
}

Status UbTentTransport::submitRemoteSlice(UbTask& task, UbSlice& slice) {
    if (!ub_primitive_) {
        return Status::InternalError(
            "UbTentTransport: native UB primitive is not installed" LOC_MARK);
    }
    if (slice.remote.remote_eid.empty() ||
        slice.remote.remote_jetty_num.empty() ||
        slice.remote.remote_tseg.empty() ||
        slice.remote.peer_nic_path.empty()) {
        return Status::InvalidArgument(
            "UbTentTransport: remote UB metadata is incomplete" LOC_MARK);
    }

    uint32_t l_seg_index = 0;
    CHECK_STATUS(findLocalSegment(reinterpret_cast<uint64_t>(slice.source_addr),
                                  slice.length, slice.selected_device_id,
                                  l_seg_index));

    mooncake::UbTransport::NativeSliceDesc desc;
    desc.opcode = slice.opcode == Request::READ
                      ? mooncake::UbTransport::NativeOpCode::Read
                      : mooncake::UbTransport::NativeOpCode::Write;
    desc.source = slice.source_addr;
    desc.target_addr = slice.target_addr;
    desc.length = slice.length;
    desc.device_id = slice.selected_device_id;
    desc.local_seg_index = l_seg_index;
    desc.peer_nic_path = slice.remote.peer_nic_path;
    desc.remote_eid = slice.remote.remote_eid;
    desc.remote_jetty_num = slice.remote.remote_jetty_num;
    desc.remote_tseg = slice.remote.remote_tseg;
    desc.retry_cnt = 0;
    desc.max_retry_cnt =
        conf_ ? conf_->get<uint32_t>("transports/ub/retry_count", 3) : 3;
    desc.completion = [this, &task, &slice](bool success, size_t bytes) {
        std::lock_guard<std::mutex> lock(task.mutex);
        if (success) {
            slice.transferred_bytes = bytes;
            completeSlice(slice, TransferStatusEnum::COMPLETED);
        } else {
            completeSlice(slice, TransferStatusEnum::FAILED);
        }
        aggregateTask(task);
    };
    return ub_primitive_->submitNativeSlice(desc);
}

void UbTentTransport::completeSlice(UbSlice& slice, TransferStatusEnum status) {
    if (slice.status == TransferStatusEnum::COMPLETED ||
        slice.status == TransferStatusEnum::FAILED ||
        slice.status == TransferStatusEnum::TIMEOUT) {
        return;
    }
    slice.status = status;
    slice.complete_ns = currentTimeNs();
    if (slice.selected_device_id >= 0) {
        std::lock_guard<std::mutex> lock(device_mutex_);
        if (static_cast<size_t>(slice.selected_device_id) < devices_.size()) {
            auto& dev = devices_[slice.selected_device_id];
            dev->inflight_bytes.fetch_sub(slice.length,
                                          std::memory_order_relaxed);
            if (status == TransferStatusEnum::COMPLETED) {
                slice.transferred_bytes = slice.length;
                dev->completed_bytes.fetch_add(slice.length,
                                               std::memory_order_relaxed);
                uint64_t elapsed_ns = slice.complete_ns > slice.submit_ns
                                          ? slice.complete_ns - slice.submit_ns
                                          : 1;
                double observed = static_cast<double>(slice.length) /
                                  (static_cast<double>(elapsed_ns) / 1e9);
                double old =
                    dev->ewma_bandwidth_bps.load(std::memory_order_relaxed);
                double next = 0.01 * old + 0.99 * observed;
                dev->ewma_bandwidth_bps.store(next, std::memory_order_relaxed);
            } else {
                dev->failed_count.fetch_add(1, std::memory_order_relaxed);
                dev->last_error_ns.store(slice.complete_ns,
                                         std::memory_order_relaxed);
            }
        }
    }
}

void UbTentTransport::aggregateTask(UbTask& task) {
    size_t transferred = 0;
    bool any_failed = false;
    bool any_pending = false;
    for (const auto& slice : task.slices) {
        transferred += slice.transferred_bytes;
        if (slice.status == TransferStatusEnum::FAILED ||
            slice.status == TransferStatusEnum::TIMEOUT ||
            slice.status == TransferStatusEnum::CANCELED ||
            slice.status == TransferStatusEnum::INVALID) {
            any_failed = true;
        } else if (slice.status != TransferStatusEnum::COMPLETED) {
            any_pending = true;
        }
    }
    task.transferred_bytes = transferred;
    if (any_failed) {
        task.status = TransferStatusEnum::FAILED;
    } else if (any_pending) {
        task.status = TransferStatusEnum::PENDING;
    } else {
        task.status = TransferStatusEnum::COMPLETED;
    }
}

Status UbTentTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    if (!installed_) {
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
    }
    if (ub_batch->task_list.size() + request_list.size() > ub_batch->max_size) {
        return Status::TooManyRequests(
            "UbTentTransport: exceed batch capacity" LOC_MARK);
    }

    const size_t block = sliceSize();
    for (const auto& req : request_list) {
        auto task = std::make_unique<UbTask>();
        task->request = req;
        task->status = TransferStatusEnum::PENDING;
        const size_t num_slices =
            std::max<size_t>(1, (req.length + block - 1) / block);
        task->slices.reserve(num_slices);

        size_t offset = 0;
        while (offset < req.length || (req.length == 0 && offset == 0)) {
            size_t length =
                req.length == 0 ? 0 : std::min(block, req.length - offset);
            int device_id = -1;
            CHECK_STATUS(chooseDevice(ub_batch->device_mask, length,
                                      req.priority, device_id));
            UbRemoteMetadata remote;
            auto metadata_status =
                resolveRemoteMetadata(req, offset, length, device_id, remote);
            if (!metadata_status.ok()) {
                UbSlice failed_slice;
                failed_slice.length = length;
                failed_slice.selected_device_id = device_id;
                failed_slice.submit_ns = currentTimeNs();
                completeSlice(failed_slice, TransferStatusEnum::FAILED);
                return metadata_status;
            }

            task->slices.emplace_back();
            auto& slice = task->slices.back();
            slice.opcode = req.opcode;
            slice.source_addr = static_cast<char*>(req.source) + offset;
            slice.target_addr = req.target_offset + offset;
            slice.length = length;
            slice.target_id = req.target_id;
            slice.remote = std::move(remote);
            slice.priority = req.priority;
            slice.selected_device_id = device_id;
            slice.status = TransferStatusEnum::PENDING;
            slice.enqueue_ns = currentTimeNs();
            slice.submit_ns = slice.enqueue_ns;

            if (req.target_id == LOCAL_SEGMENT_ID) {
                if (req.opcode == Request::WRITE) {
                    std::memcpy(reinterpret_cast<void*>(slice.target_addr),
                                slice.source_addr, length);
                } else {
                    std::memcpy(slice.source_addr,
                                reinterpret_cast<void*>(slice.target_addr),
                                length);
                }
                completeSlice(slice, TransferStatusEnum::COMPLETED);
            } else {
                auto submit_status = submitRemoteSlice(*task, slice);
                if (!submit_status.ok()) {
                    LOG(WARNING)
                        << "UbTentTransport: native slice submit failed: "
                        << submit_status.message();
                    completeSlice(slice, TransferStatusEnum::FAILED);
                }
            }

            if (req.length == 0) break;
            offset += length;
        }

        {
            std::lock_guard<std::mutex> lock(task->mutex);
            aggregateTask(*task);
        }
        ub_batch->task_list.push_back(std::move(task));
    }

    return Status::OK();
}

Status UbTentTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    if (task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid task id" LOC_MARK);
    }

    auto& task = *ub_batch->task_list[task_id];
    std::lock_guard<std::mutex> lock(task.mutex);
    aggregateTask(task);
    status.s = task.status;
    status.transferred_bytes = task.transferred_bytes;
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
