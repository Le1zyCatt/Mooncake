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
constexpr uint64_t kUbFailureCooldownNs = 100ull * 1000 * 1000;

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

struct UbBufferAttrs {
    std::vector<std::string> tseg;
    std::vector<uint32_t> l_seg_index;
};

struct UbDeviceAttrs {
    std::string eid;
    std::vector<uint32_t> jetty_num;
    int device_id{-1};
};

Status parseUbBufferAttrs(const std::string& attrs, UbBufferAttrs& out,
                          const std::string& context) {
    try {
        auto j = json::parse(attrs);
        if (!j.is_object() || j.value("version", 0) != 1 ||
            !j.contains("buffers") || !j["buffers"].is_object()) {
            return Status::InvalidArgument(
                context + ": invalid UB buffer attrs envelope");
        }
        const auto& buffers = j["buffers"];
        if (!buffers.contains("tseg") || !buffers["tseg"].is_array()) {
            return Status::InvalidArgument(context +
                                           ": UB buffer attrs missing tseg");
        }
        for (const auto& value : buffers["tseg"]) {
            if (!value.is_string()) {
                return Status::InvalidArgument(context +
                                               ": UB tseg entry is not string");
            }
            out.tseg.push_back(value.get<std::string>());
        }
        if (buffers.contains("l_seg_index")) {
            if (!buffers["l_seg_index"].is_array()) {
                return Status::InvalidArgument(
                    context + ": UB l_seg_index is not an array");
            }
            for (const auto& value : buffers["l_seg_index"]) {
                if (!value.is_number_unsigned()) {
                    return Status::InvalidArgument(
                        context + ": UB l_seg_index entry is not uint");
                }
                out.l_seg_index.push_back(value.get<uint32_t>());
            }
        }
        if (out.tseg.empty()) {
            return Status::InvalidArgument(context + ": UB tseg is empty");
        }
        if (!out.l_seg_index.empty() &&
            out.l_seg_index.size() != out.tseg.size()) {
            return Status::InvalidArgument(
                context + ": UB tseg/l_seg_index size mismatch");
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(
            context + ": malformed UB buffer attrs: " + e.what());
    }
}

Status parseUbDeviceAttrs(const std::string& attrs, UbDeviceAttrs& out,
                          const std::string& context) {
    try {
        auto j = json::parse(attrs);
        if (!j.is_object() || j.value("version", 0) != 1) {
            return Status::InvalidArgument(
                context + ": invalid UB device attrs envelope");
        }
        if (!j.contains("eid") || !j["eid"].is_string()) {
            return Status::InvalidArgument(context +
                                           ": UB device attrs missing eid");
        }
        out.eid = j["eid"].get<std::string>();
        if (out.eid.empty()) {
            return Status::InvalidArgument(context +
                                           ": UB device eid is empty");
        }
        if (!j.contains("jetty_num") || !j["jetty_num"].is_array()) {
            return Status::InvalidArgument(
                context + ": UB device attrs missing jetty_num");
        }
        for (const auto& value : j["jetty_num"]) {
            if (!value.is_number_unsigned()) {
                return Status::InvalidArgument(
                    context + ": UB jetty_num entry is not uint");
            }
            out.jetty_num.push_back(value.get<uint32_t>());
        }
        if (out.jetty_num.empty()) {
            return Status::InvalidArgument(context + ": UB jetty_num is empty");
        }
        if (j.contains("device_id")) out.device_id = j["device_id"].get<int>();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(
            context + ": malformed UB device attrs: " + e.what());
    }
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

    ub_primitive_ = std::make_unique<UbNativePrimitive>();
    auto install_status =
        ub_primitive_->install(local_segment_name_, device_names);
    if (!install_status.ok()) {
        ub_primitive_.reset();
        return install_status;
    }
    auto native_devices = ub_primitive_->devices();
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
    accepting_submissions_ = true;

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
    accepting_submissions_ = false;
    installed_ = false;
    drainAllInflight();
    if (ub_primitive_) {
        ub_primitive_->uninstall();
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
    auto native_devices = ub_primitive_
                              ? ub_primitive_->devices()
                              : std::vector<UbNativePrimitive::DeviceDesc>();
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
    if (!accepting_submissions_) {
        return Status::InternalError(
            "UbTentTransport: not accepting new submissions" LOC_MARK);
    }

    if (!ub_primitive_) {
        return Status::InternalError(
            "UbTentTransport: native UB primitive is not installed" LOC_MARK);
    }

    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        for (auto& entry : registered_buffers_) {
            if (entry->addr == desc.addr && entry->length == desc.length) {
                entry->ref_count++;
                desc.location = options.location.empty() ? kWildcardLocation
                                                         : options.location;
                desc.transport_attrs[TransportType::UB] =
                    makeUbBufferAttrs(entry->tseg, entry->l_seg_index).dump();
                if (!containsTransport(desc.transports, TransportType::UB)) {
                    desc.transports.push_back(TransportType::UB);
                }
                return Status::OK();
            }
        }
    }

    // URMA requires the registered host VA range to be acceptable to the driver
    // (typically page-aligned on Kunpeng). UbTentTransport preserves the TENT
    // BufferDesc addr/length exactly and lets the primitive reject unsupported
    // ranges; it does not silently round the user-visible range.
    UbNativePrimitive::MemoryRegistration native_desc;
    CHECK_STATUS(ub_primitive_->registerMemory(
        reinterpret_cast<void*>(desc.addr), desc.length, native_desc));

    desc.location =
        options.location.empty() ? kWildcardLocation : options.location;
    desc.transport_attrs[TransportType::UB] =
        makeUbBufferAttrs(native_desc.tseg, native_desc.l_seg_index).dump();
    if (!containsTransport(desc.transports, TransportType::UB)) {
        desc.transports.push_back(TransportType::UB);
    }
    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        registered_buffers_.push_back(std::make_shared<LocalBufferRegistration>(
            LocalBufferRegistration{desc.addr, desc.length, native_desc.tseg,
                                    native_desc.l_seg_index, 1, 0}));
    }
    return Status::OK();
}

Status UbTentTransport::removeMemoryBuffer(BufferDesc& desc) {
    bool should_unregister = false;
    {
        std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
        for (auto it = registered_buffers_.begin();
             it != registered_buffers_.end(); ++it) {
            auto& entry = **it;
            if (entry.addr != desc.addr) continue;
            // Native completions hold an in-flight reference while the
            // low-level URMA Slice may still use the local segment handle.
            // Refuse removal instead of unregistering memory underneath posted
            // work.
            if (entry.in_flight_slices != 0) {
                return Status::TooManyRequests(
                    "UbTentTransport: cannot remove UB buffer with in-flight "
                    "slices" LOC_MARK);
            }
            if (entry.ref_count > 1) {
                entry.ref_count--;
            } else {
                registered_buffers_.erase(it);
                should_unregister = true;
            }
            break;
        }
    }
    if (should_unregister && ub_primitive_) {
        ub_primitive_->unregisterMemory(reinterpret_cast<void*>(desc.addr));
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
    for (const auto& task : ub_batch->task_list) drainTask(task);
    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

size_t UbTentTransport::sliceSize() const {
    if (!conf_) return kDefaultUbSliceSize;
    size_t value = conf_->get<size_t>("transports/ub/workers/block_size", 0);
    if (value == 0) value = conf_->get<size_t>("transports/ub/slice_size", 0);
    return value == 0 ? kDefaultUbSliceSize : value;
}

uint64_t UbTentTransport::currentTimeNs() const {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

void UbTentTransport::drainTask(const std::shared_ptr<UbTask>& task) {
    if (!task) return;
    std::unique_lock<std::mutex> lock(task->mutex);
    task->cv.wait(lock, [&] { return task->in_flight_slices == 0; });
}

void UbTentTransport::drainAllInflight() {
    std::unique_lock<std::mutex> lock(inflight_mutex_);
    inflight_cv_.wait(lock, [&] { return in_flight_slices_ == 0; });
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

void UbTentTransport::releaseDeviceInflight(int device_id, size_t length) {
    if (device_id < 0) return;
    std::lock_guard<std::mutex> lock(device_mutex_);
    if (static_cast<size_t>(device_id) >= devices_.size()) return;
    auto& inflight = devices_[device_id]->inflight_bytes;
    auto current = inflight.load(std::memory_order_relaxed);
    inflight.store(current > length ? current - length : 0,
                   std::memory_order_relaxed);
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

    std::string preferred_device_name;
    {
        std::lock_guard<std::mutex> lock(device_mutex_);
        if (preferred_device_id < 0 ||
            static_cast<size_t>(preferred_device_id) >= devices_.size()) {
            return Status::DeviceNotFound(
                "UbTentTransport: selected UB device is invalid" LOC_MARK);
        }
        preferred_device_name = devices_[preferred_device_id]->name;
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
                "UbTentTransport: segment " + std::to_string(req.target_id) +
                " buffer " + std::to_string(i) + " lacks UB attrs" LOC_MARK);
        }

        UbBufferAttrs buffer_attrs;
        CHECK_STATUS(parseUbBufferAttrs(it->second, buffer_attrs,
                                        "UbTentTransport segment " +
                                            std::to_string(req.target_id) +
                                            " buffer " + std::to_string(i)));
        if (buffer_attrs.tseg.size() != mem.devices.size()) {
            return Status::InvalidArgument(
                "UbTentTransport: segment " + std::to_string(req.target_id) +
                " buffer " + std::to_string(i) +
                " UB tseg/device count mismatch" LOC_MARK);
        }

        std::optional<size_t> remote_device_index;
        UbDeviceAttrs selected_device_attrs;
        for (size_t device_index = 0; device_index < mem.devices.size();
             ++device_index) {
            const auto& dev = mem.devices[device_index];
            auto dit = dev.transport_attrs.find(TransportType::UB);
            if (dit == dev.transport_attrs.end()) continue;
            UbDeviceAttrs device_attrs;
            auto parse_status = parseUbDeviceAttrs(
                dit->second, device_attrs,
                "UbTentTransport segment " + std::to_string(req.target_id) +
                    " device " + dev.name);
            if (!parse_status.ok()) return parse_status;
            if (dev.name == preferred_device_name ||
                device_attrs.device_id == preferred_device_id) {
                remote_device_index = device_index;
                selected_device_attrs = std::move(device_attrs);
                break;
            }
        }
        if (!remote_device_index.has_value()) {
            return Status::InvalidArgument(
                "UbTentTransport: segment " + std::to_string(req.target_id) +
                " buffer " + std::to_string(i) +
                " has no UB device matching local device " +
                preferred_device_name + " (id " +
                std::to_string(preferred_device_id) + ")" LOC_MARK);
        }

        const auto& dev = mem.devices[*remote_device_index];
        remote.target_buffer_index = i;
        remote.remote_device_id = static_cast<int>(*remote_device_index);
        remote.remote_device_name = dev.name;
        remote.remote_tseg = buffer_attrs.tseg[*remote_device_index];
        remote.peer_nic_path = target->nicPathServerName() + "@" + dev.name;
        remote.remote_eid = std::move(selected_device_attrs.eid);
        remote.remote_jetty_num = std::move(selected_device_attrs.jetty_num);
        return Status::OK();
    }

    return Status::InvalidArgument(
        "UbTentTransport: target address not in registered buffer" LOC_MARK);
}

Status UbTentTransport::reserveLocalSegment(
    uint64_t addr, size_t length, int device_id, uint32_t& l_seg_index,
    std::shared_ptr<LocalBufferRegistration>& registration) {
    std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
    for (const auto& buffer : registered_buffers_) {
        if (addr < buffer->addr || length > buffer->length ||
            addr - buffer->addr > buffer->length - length) {
            continue;
        }
        if (device_id < 0 ||
            static_cast<size_t>(device_id) >= buffer->l_seg_index.size()) {
            return Status::DeviceNotFound(
                "UbTentTransport: selected UB device is not "
                "registered" LOC_MARK);
        }
        buffer->in_flight_slices++;
        registration = buffer;
        l_seg_index = buffer->l_seg_index[device_id];
        return Status::OK();
    }
    return Status::AddressNotRegistered(
        "UbTentTransport: local source buffer is not registered for "
        "UB" LOC_MARK);
}

void UbTentTransport::releaseLocalSegment(
    const std::shared_ptr<LocalBufferRegistration>& registration) {
    if (!registration) return;
    std::lock_guard<std::mutex> lock(registered_buffers_mutex_);
    if (registration->in_flight_slices > 0) registration->in_flight_slices--;
}

Status UbTentTransport::submitRemoteSlice(
    const std::shared_ptr<UbTask>& task,
    const std::shared_ptr<UbSlice>& slice) {
    if (!ub_primitive_) {
        return Status::InternalError(
            "UbTentTransport: native UB primitive is not installed" LOC_MARK);
    }
    if (slice->remote.remote_eid.empty() ||
        slice->remote.remote_jetty_num.empty() ||
        slice->remote.remote_tseg.empty() ||
        slice->remote.peer_nic_path.empty()) {
        return Status::InvalidArgument(
            "UbTentTransport: remote UB metadata is incomplete" LOC_MARK);
    }

    uint32_t l_seg_index = 0;
    std::shared_ptr<LocalBufferRegistration> registration;
    CHECK_STATUS(reserveLocalSegment(
        reinterpret_cast<uint64_t>(slice->source_addr), slice->length,
        slice->selected_device_id, l_seg_index, registration));

    {
        std::lock_guard<std::mutex> lock(task->mutex);
        task->in_flight_slices++;
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        in_flight_slices_++;
    }

    UbNativePrimitive::SliceDesc desc;
    desc.opcode = slice->opcode == Request::READ
                      ? UbNativePrimitive::OpCode::Read
                      : UbNativePrimitive::OpCode::Write;
    desc.source = slice->source_addr;
    desc.target_addr = slice->target_addr;
    desc.length = slice->length;
    desc.device_id = slice->selected_device_id;
    desc.local_seg_index = l_seg_index;
    desc.peer_nic_path = slice->remote.peer_nic_path;
    desc.remote_eid = slice->remote.remote_eid;
    desc.remote_jetty_num = slice->remote.remote_jetty_num;
    desc.remote_tseg = slice->remote.remote_tseg;
    desc.retry_cnt = 0;
    desc.max_retry_cnt =
        conf_ ? conf_->get<uint32_t>("transports/ub/retry_count", 3) : 3;
    desc.completion = [this, task, slice, registration](bool success,
                                                        size_t bytes) {
        finishRemoteSlice(task, slice, registration,
                          success ? TransferStatusEnum::COMPLETED
                                  : TransferStatusEnum::FAILED,
                          bytes);
    };
    auto status = ub_primitive_->submitSlice(desc);
    if (!status.ok()) {
        finishRemoteSlice(task, slice, registration, TransferStatusEnum::FAILED,
                          0);
    }
    return status;
}

void UbTentTransport::finishRemoteSlice(
    const std::shared_ptr<UbTask>& task, const std::shared_ptr<UbSlice>& slice,
    const std::shared_ptr<LocalBufferRegistration>& registration,
    TransferStatusEnum status, size_t transferred_bytes) {
    {
        std::lock_guard<std::mutex> lock(task->mutex);
        slice->transferred_bytes = transferred_bytes;
        completeSlice(*slice, status);
        aggregateTask(*task);
        if (task->in_flight_slices > 0) task->in_flight_slices--;
    }
    task->cv.notify_all();
    releaseLocalSegment(registration);
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        if (in_flight_slices_ > 0) in_flight_slices_--;
    }
    inflight_cv_.notify_all();
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
            auto current = dev->inflight_bytes.load(std::memory_order_relaxed);
            dev->inflight_bytes.store(
                current > slice.length ? current - slice.length : 0,
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
                double next = 0.9 * old + 0.1 * observed;
                dev->ewma_bandwidth_bps.store(next, std::memory_order_relaxed);
            } else {
                dev->failed_count.fetch_add(1, std::memory_order_relaxed);
                dev->last_error_ns.store(slice.complete_ns,
                                         std::memory_order_relaxed);
                dev->cooldown_until_ns.store(
                    slice.complete_ns + kUbFailureCooldownNs,
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
        transferred += slice->transferred_bytes;
        if (slice->status == TransferStatusEnum::FAILED ||
            slice->status == TransferStatusEnum::TIMEOUT ||
            slice->status == TransferStatusEnum::CANCELED ||
            slice->status == TransferStatusEnum::INVALID) {
            any_failed = true;
        } else if (slice->status != TransferStatusEnum::COMPLETED) {
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
    if (!accepting_submissions_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "UbTentTransport: not accepting new submissions" LOC_MARK);
    }
    if (ub_batch->task_list.size() + request_list.size() > ub_batch->max_size) {
        return Status::TooManyRequests(
            "UbTentTransport: exceed batch capacity" LOC_MARK);
    }

    const size_t block = sliceSize();
    for (const auto& req : request_list) {
        // UB-internal chunking: this is the work-request granularity used by
        // the UB primitive, not TENT cross-transport partitioning. If any
        // chunk of a task fails, the task is FAILED and already-posted chunks
        // are not rolled back; callers must treat only COMPLETED as full remote
        // buffer visibility.
        auto task = std::make_shared<UbTask>();
        task->request = req;
        task->status = TransferStatusEnum::PENDING;
        const size_t num_slices =
            std::max<size_t>(1, (req.length + block - 1) / block);
        task->slices.reserve(num_slices);
        ub_batch->task_list.push_back(task);

        Status first_error = Status::OK();
        size_t offset = 0;
        while (offset < req.length || (req.length == 0 && offset == 0)) {
            size_t length =
                req.length == 0 ? 0 : std::min(block, req.length - offset);
            int device_id = -1;
            auto choose_status = chooseDevice(ub_batch->device_mask, length,
                                              req.priority, device_id);
            if (!choose_status.ok()) {
                auto failed_slice = std::make_shared<UbSlice>();
                failed_slice->id = task->slices.size();
                failed_slice->length = length;
                failed_slice->submit_ns = currentTimeNs();
                completeSlice(*failed_slice, TransferStatusEnum::FAILED);
                task->slices.push_back(failed_slice);
                first_error = choose_status;
                break;
            }
            UbRemoteMetadata remote;
            auto metadata_status =
                resolveRemoteMetadata(req, offset, length, device_id, remote);
            if (!metadata_status.ok()) {
                auto failed_slice = std::make_shared<UbSlice>();
                failed_slice->id = task->slices.size();
                failed_slice->length = length;
                failed_slice->selected_device_id = device_id;
                failed_slice->submit_ns = currentTimeNs();
                completeSlice(*failed_slice, TransferStatusEnum::FAILED);
                task->slices.push_back(failed_slice);
                first_error = metadata_status;
                break;
            }

            auto slice = std::make_shared<UbSlice>();
            slice->id = task->slices.size();
            slice->opcode = req.opcode;
            slice->source_addr = static_cast<char*>(req.source) + offset;
            slice->target_addr = req.target_offset + offset;
            slice->length = length;
            slice->target_id = req.target_id;
            slice->remote = std::move(remote);
            slice->priority = req.priority;
            slice->selected_device_id = device_id;
            slice->status = TransferStatusEnum::PENDING;
            slice->enqueue_ns = currentTimeNs();
            slice->submit_ns = slice->enqueue_ns;
            task->slices.push_back(slice);

            if (req.target_id == LOCAL_SEGMENT_ID) {
                if (req.opcode == Request::WRITE) {
                    std::memcpy(reinterpret_cast<void*>(slice->target_addr),
                                slice->source_addr, length);
                } else {
                    std::memcpy(slice->source_addr,
                                reinterpret_cast<void*>(slice->target_addr),
                                length);
                }
                completeSlice(*slice, TransferStatusEnum::COMPLETED);
            } else {
                auto submit_status = submitRemoteSlice(task, slice);
                if (!submit_status.ok()) {
                    LOG(WARNING)
                        << "UbTentTransport: native slice submit failed: "
                        << submit_status.message();
                    first_error = submit_status;
                    break;
                }
            }

            if (req.length == 0) break;
            offset += length;
        }

        {
            std::lock_guard<std::mutex> lock(task->mutex);
            aggregateTask(*task);
        }
        if (!first_error.ok()) return first_error;
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
