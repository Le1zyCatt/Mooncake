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
#ifndef TENT_TRANSPORT_UB_NATIVE_PRIMITIVE_H
#define TENT_TRANSPORT_UB_NATIVE_PRIMITIVE_H

#include "tent/common/status.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {
class UbTransport;

namespace tent {

// UbNativePrimitive is the narrow adapter between TENT-native UB scheduling and
// the existing URMA implementation. It only exposes resource registration,
// endpoint setup metadata, post-send, and completion callbacks. It deliberately
// does not expose classic TE task or batch semantics to UbTentTransport.
class UbNativePrimitive {
   public:
    struct DeviceDesc {
        std::string name;
        std::string eid;
        std::vector<uint32_t> jetty_num;
    };

    struct MemoryRegistration {
        std::vector<std::string> tseg;
        std::vector<uint32_t> l_seg_index;
    };

    enum class OpCode { Read, Write };

    struct SliceDesc {
        OpCode opcode{OpCode::Read};
        void* source{nullptr};
        uint64_t target_addr{0};
        size_t length{0};
        int device_id{-1};
        uint32_t local_seg_index{0};
        std::string peer_nic_path;
        std::string remote_eid;
        std::vector<uint32_t> remote_jetty_num;
        std::string remote_tseg;
        uint32_t retry_cnt{0};
        uint32_t max_retry_cnt{3};
        std::function<void(bool success, size_t bytes)> completion;
    };

    UbNativePrimitive();
    ~UbNativePrimitive();

    Status install(const std::string& local_segment_name,
                   const std::vector<std::string>& device_names);
    void uninstall();

    std::vector<DeviceDesc> devices();
    Status registerMemory(void* addr, size_t length,
                          MemoryRegistration& registration);
    void unregisterMemory(void* addr);
    Status submitSlice(const SliceDesc& desc);

   private:
    std::unique_ptr<mooncake::UbTransport> impl_;
    bool installed_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_NATIVE_PRIMITIVE_H
