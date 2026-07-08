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
#ifndef TENT_TRANSPORT_UB_METADATA_ATTRS_H
#define TENT_TRANSPORT_UB_METADATA_ATTRS_H

#include "tent/common/status.h"
#include "tent/thirdparty/nlohmann/json.h"

#include <cstdint>
#include <exception>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {

struct UbBufferAttrs {
    std::vector<std::string> tseg;
    std::vector<uint32_t> l_seg_index;
};

struct UbDeviceAttrs {
    std::string eid;
    std::vector<uint32_t> jetty_num;
    int device_id{-1};
};

inline Status parseUbBufferAttrs(const std::string& attrs, UbBufferAttrs& out,
                                 const std::string& context) {
    using json = nlohmann::json;
    out = UbBufferAttrs{};
    try {
        auto j = json::parse(attrs);
        if (!j.is_object()) {
            return Status::InvalidArgument(
                context + ": invalid UB buffer attrs envelope");
        }
        if (!j.contains("version") || !j["version"].is_number_integer() ||
            j["version"].get<int>() != 1) {
            return Status::InvalidArgument(
                context + ": unsupported UB buffer attrs version");
        }
        if (!j.contains("buffers") || !j["buffers"].is_object()) {
            return Status::InvalidArgument(context +
                                           ": UB buffer attrs missing buffers");
        }
        const auto& buffers = j["buffers"];
        if (!buffers.contains("tseg") || !buffers["tseg"].is_array()) {
            return Status::InvalidArgument(context +
                                           ": UB buffer attrs missing tseg");
        }
        if (!buffers.contains("l_seg_index") ||
            !buffers["l_seg_index"].is_array()) {
            return Status::InvalidArgument(
                context + ": UB buffer attrs missing l_seg_index");
        }
        for (const auto& value : buffers["tseg"]) {
            if (!value.is_string()) {
                return Status::InvalidArgument(context +
                                               ": UB tseg entry is not string");
            }
            out.tseg.push_back(value.get<std::string>());
        }
        for (const auto& value : buffers["l_seg_index"]) {
            if (!value.is_number_unsigned()) {
                return Status::InvalidArgument(
                    context + ": UB l_seg_index entry is not uint");
            }
            out.l_seg_index.push_back(value.get<uint32_t>());
        }
        if (out.tseg.empty()) {
            return Status::InvalidArgument(context + ": UB tseg is empty");
        }
        if (out.l_seg_index.size() != out.tseg.size()) {
            return Status::InvalidArgument(
                context + ": UB tseg/l_seg_index size mismatch");
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(
            context + ": malformed UB buffer attrs: " + e.what());
    }
}

inline Status parseUbDeviceAttrs(const std::string& attrs, UbDeviceAttrs& out,
                                 const std::string& context) {
    using json = nlohmann::json;
    out = UbDeviceAttrs{};
    try {
        auto j = json::parse(attrs);
        if (!j.is_object()) {
            return Status::InvalidArgument(
                context + ": invalid UB device attrs envelope");
        }
        if (!j.contains("version") || !j["version"].is_number_integer() ||
            j["version"].get<int>() != 1) {
            return Status::InvalidArgument(
                context + ": unsupported UB device attrs version");
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
        if (!j.contains("device_id") || !j["device_id"].is_number_integer()) {
            return Status::InvalidArgument(
                context + ": UB device attrs missing device_id");
        }
        out.device_id = j["device_id"].get<int>();
        if (out.device_id < 0) {
            return Status::InvalidArgument(
                context + ": UB device_id must be non-negative");
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(
            context + ": malformed UB device attrs: " + e.what());
    }
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_METADATA_ATTRS_H
