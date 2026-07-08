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

#include "tent/transport/ub/ub_native_primitive.h"

#include "topology.h"
#include "transport/kunpeng_transport/ub_transport.h"

namespace mooncake {
namespace tent {

UbNativePrimitive::UbNativePrimitive()
    : impl_(std::make_unique<mooncake::UbTransport>()) {}

UbNativePrimitive::~UbNativePrimitive() { uninstall(); }

Status UbNativePrimitive::install(
    const std::string& local_segment_name,
    const std::vector<std::string>& device_names) {
    auto topology = std::make_shared<mooncake::Topology>();
    if (!device_names.empty()) topology->discover(device_names);
    std::string segment_name = local_segment_name;
    int rc = impl_->installPrimitive(segment_name, topology);
    if (rc) {
        return Status::DeviceNotFound(
            "UbNativePrimitive: failed to initialize UB primitive");
    }
    installed_ = true;
    return Status::OK();
}

void UbNativePrimitive::uninstall() {
    if (impl_ && installed_) {
        impl_->uninstallPrimitive();
        installed_ = false;
    }
}

std::vector<UbNativePrimitive::DeviceDesc> UbNativePrimitive::devices() {
    std::vector<DeviceDesc> out;
    for (const auto& native : impl_->nativeDevices()) {
        out.push_back(DeviceDesc{native.name, native.eid, native.jetty_num});
    }
    return out;
}

Status UbNativePrimitive::registerMemory(void* addr, size_t length,
                                         MemoryRegistration& registration) {
    mooncake::UbTransport::NativeMemoryDesc native;
    int rc = impl_->registerLocalMemoryNative(addr, length, native);
    if (rc) {
        return Status::AddressNotRegistered(
            "UbNativePrimitive: UB memory registration failed");
    }
    registration.tseg = std::move(native.tseg);
    registration.l_seg_index = std::move(native.l_seg_index);
    return Status::OK();
}

void UbNativePrimitive::unregisterMemory(void* addr) {
    impl_->unregisterLocalMemoryNative(addr);
}

Status UbNativePrimitive::submitSlice(const SliceDesc& desc) {
    mooncake::UbTransport::NativeSliceDesc native;
    native.opcode = desc.opcode == OpCode::Read
                        ? mooncake::UbTransport::NativeOpCode::Read
                        : mooncake::UbTransport::NativeOpCode::Write;
    native.source = desc.source;
    native.target_addr = desc.target_addr;
    native.length = desc.length;
    native.device_id = desc.device_id;
    native.local_seg_index = desc.local_seg_index;
    native.peer_nic_path = desc.peer_nic_path;
    native.remote_eid = desc.remote_eid;
    native.remote_jetty_num = desc.remote_jetty_num;
    native.remote_tseg = desc.remote_tseg;
    native.retry_cnt = desc.retry_cnt;
    native.max_retry_cnt = desc.max_retry_cnt;
    native.completion = desc.completion;
    auto status = impl_->submitNativeSlice(native);
    if (status.ok()) return Status::OK();
    return Status::InternalError("UbNativePrimitive: " + status.ToString());
}

}  // namespace tent
}  // namespace mooncake
