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
//
// Unit tests for UbTentTransport and the "ub" selector string mapping.
//
// These tests validate the TENT-native UB execution model. They do not require
// real Kunpeng hardware because the unit scope exercises native task/slice
// creation, metadata publication, device-mask selection, and local mock copies.
//
// To build and run:
//   cmake -DUSE_UB=ON -DUSE_TENT=ON ...
//   ctest -R tent_ub_transport_test -V

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/transport_selector.h"
#include "tent/runtime/segment.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/ub_tent_transport.h"

namespace mooncake {
namespace tent {
namespace {

using json = nlohmann::json;

void* alignedAlloc(size_t bytes) {
    void* ptr = nullptr;
    EXPECT_EQ(posix_memalign(&ptr, 4096, bytes), 0);
    if (ptr) std::memset(ptr, 0, bytes);
    return ptr;
}

// ---------------------------------------------------------------------------
// 1.  Selector / string-mapping tests (no hardware required)
// ---------------------------------------------------------------------------

TEST(UbSelectorTest, TypeNameRoundTrip) {
    EXPECT_EQ(TransportSelector::transportTypeName(UB), "ub");
}

TEST(UbSelectorTest, ParseUbString) {
    EXPECT_EQ(TransportSelector::parseTransportType("ub"), UB);
}

TEST(UbSelectorTest, ParseUnknownStillReturnsUnspec) {
    EXPECT_EQ(TransportSelector::parseTransportType("ub_typo"), UNSPEC);
}

TEST(UbSelectorTest, UbEnumValue) {
    // UB must be between SUNRISE_LINK and kNumTransportTypes.
    EXPECT_GT(static_cast<int>(UB), static_cast<int>(SUNRISE_LINK));
    EXPECT_LT(static_cast<int>(UB), static_cast<int>(kNumTransportTypes));
}

class MinimalFakeTransport : public Transport {
   public:
    void setDram() { caps.dram_to_dram = true; }
    Status allocateSubBatch(SubBatchRef&, size_t) override {
        return Status::OK();
    }
    Status freeSubBatch(SubBatchRef&) override { return Status::OK(); }
    Status submitTransferTasks(SubBatchRef,
                               const std::vector<Request>&) override {
        return Status::OK();
    }
    Status getTransferStatus(SubBatchRef, int, TransferStatus&) override {
        return Status::OK();
    }
    const char* getName() const override { return "fake"; }
};

SelectionContext makeRemoteCpuSelectionContext() {
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = 0;
    ctx.buffer_transports = nullptr;
    return ctx;
}

std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
makeUbTcpTransports() {
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    auto ub_fake = std::make_shared<MinimalFakeTransport>();
    ub_fake->setDram();
    auto tcp_fake = std::make_shared<MinimalFakeTransport>();
    tcp_fake->setDram();
    transports[UB] = ub_fake;
    transports[TCP] = tcp_fake;
    return transports;
}

// A policy JSON with only "ub" in the transports array must cause the selector
// to pick UB when both UB and TCP are available.
TEST(UbSelectorTest, SelectorPolicyForceUbWithTcpAlsoEnabled) {
    auto conf = std::make_shared<Config>();
    const std::string policy_json = R"({
        "policy": [
            {
                "name": "kunpeng_ub_memory",
                "segment_type": "memory",
                "local_memory": "cpu",
                "remote_memory": "cpu",
                "same_machine": false,
                "transports": ["ub"]
            }
        ]
    })";
    ASSERT_TRUE(conf->load(policy_json).ok());

    TransportSelector selector(conf);
    auto transports = makeUbTcpTransports();
    auto ctx = makeRemoteCpuSelectionContext();

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, UB)
        << "Selector should honor policy transports=[ub]";
}

TEST(UbSelectorTest, SelectorPolicyForceTcpDoesNotUseUb) {
    auto conf = std::make_shared<Config>();
    const std::string policy_json = R"({
        "policy": [
            {
                "name": "force_tcp_memory",
                "segment_type": "memory",
                "local_memory": "cpu",
                "remote_memory": "cpu",
                "same_machine": false,
                "transports": ["tcp"]
            }
        ]
    })";
    ASSERT_TRUE(conf->load(policy_json).ok());

    TransportSelector selector(conf);
    auto transports = makeUbTcpTransports();
    auto ctx = makeRemoteCpuSelectionContext();

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, TCP)
        << "Selector should honor policy transports=[tcp] and not use UB";
}

// ---------------------------------------------------------------------------
// 2.  UbTentTransport native control-flow tests (no network)
// ---------------------------------------------------------------------------

TEST(UbTentTransportTest, InstallWithMockUrma) {
    UbTentTransport transport;

    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);

    ASSERT_TRUE(status.ok()) << status.message();
    EXPECT_STREQ(transport.getName(), "ub");
    EXPECT_TRUE(transport.capabilities().dram_to_dram);
    EXPECT_FALSE(transport.capabilities().dram_to_gpu);
}

TEST(UbTentTransportTest, AddAndRemoveMemoryBuffer) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    // Allocate a page-aligned CPU buffer.  urma_register_seg on Kunpeng
    // hardware requires the start address to be page-aligned (4 KiB).
    // std::vector uses malloc which only guarantees 16-byte alignment, so we
    // use posix_memalign here instead.
    const size_t kBufLen = 4096;
    void* addr = alignedAlloc(kBufLen);
    ASSERT_NE(addr, nullptr);

    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(addr);
    desc.length = kBufLen;
    desc.location = "*";

    MemoryOptions opts;
    opts.perm = kGlobalReadWrite;

    auto add_s = transport.addMemoryBuffer(desc, opts);
    ASSERT_TRUE(add_s.ok()) << add_s.message();

    // UB must appear in the transport list after registration.
    auto it = std::find(desc.transports.begin(), desc.transports.end(), UB);
    EXPECT_NE(it, desc.transports.end())
        << "UB not found in desc.transports after addMemoryBuffer()";

    auto attr_it = desc.transport_attrs.find(UB);
    ASSERT_NE(attr_it, desc.transport_attrs.end());
    auto attrs = json::parse(attr_it->second);
    EXPECT_EQ(attrs.value("version", 0), 1);
    ASSERT_TRUE(attrs.contains("buffers"));
    ASSERT_TRUE(attrs["buffers"].contains("tseg"));
    EXPECT_FALSE(attrs["buffers"]["tseg"].empty());

    auto rm_s = transport.removeMemoryBuffer(desc);
    EXPECT_TRUE(rm_s.ok()) << rm_s.message();

    // UB should be removed from the transport list.
    it = std::find(desc.transports.begin(), desc.transports.end(), UB);
    EXPECT_EQ(it, desc.transports.end())
        << "UB still present in desc.transports after removeMemoryBuffer()";
    EXPECT_EQ(desc.transport_attrs.find(UB), desc.transport_attrs.end());

    free(addr);
}

TEST(UbTentTransportTest, RepeatedRegisterUnregisterSameVA) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    const size_t kBufLen = 4096;
    void* addr = alignedAlloc(kBufLen);
    ASSERT_NE(addr, nullptr);

    BufferDesc first;
    first.addr = reinterpret_cast<uint64_t>(addr);
    first.length = kBufLen;
    first.location = "*";
    BufferDesc second = first;

    MemoryOptions opts;
    opts.perm = kGlobalReadWrite;

    ASSERT_TRUE(transport.addMemoryBuffer(first, opts).ok());
    ASSERT_TRUE(transport.addMemoryBuffer(second, opts).ok());
    EXPECT_NE(first.transport_attrs.find(UB), first.transport_attrs.end());
    EXPECT_EQ(first.transport_attrs[UB], second.transport_attrs[UB]);

    EXPECT_TRUE(transport.removeMemoryBuffer(first).ok());
    EXPECT_TRUE(transport.removeMemoryBuffer(second).ok());
    free(addr);
}

TEST(UbTentTransportTest, AllocateAndFreeSubBatch) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    Transport::SubBatchRef batch = nullptr;
    auto alloc_s = transport.allocateSubBatch(batch, 8);
    ASSERT_TRUE(alloc_s.ok()) << alloc_s.message();
    ASSERT_NE(batch, nullptr);

    auto free_s = transport.freeSubBatch(batch);
    EXPECT_TRUE(free_s.ok()) << free_s.message();
    EXPECT_EQ(batch, nullptr);
}

// Submit a local-to-local native transfer and verify task status is aggregated
// from native UbSlice state.
TEST(UbTentTransportTest, SubmitAndPollMockTransfer) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    // Allocate page-aligned buffers (urma_register_seg requires 4 KiB
    // alignment).
    const size_t kBufLen = 4096;
    void* src_raw = alignedAlloc(kBufLen);
    void* dst_raw = alignedAlloc(kBufLen);
    ASSERT_NE(src_raw, nullptr);
    ASSERT_NE(dst_raw, nullptr);
    std::memset(src_raw, 0xAB, kBufLen);

    BufferDesc src_desc;
    src_desc.addr = reinterpret_cast<uint64_t>(src_raw);
    src_desc.length = kBufLen;
    src_desc.location = "*";

    MemoryOptions opts;
    opts.perm = kGlobalReadWrite;

    auto add_s = transport.addMemoryBuffer(src_desc, opts);
    if (!add_s.ok()) {
        GTEST_SKIP() << "addMemoryBuffer failed: " << add_s.message();
    }

    // Allocate a sub-batch.
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 4).ok());
    ASSERT_NE(batch, nullptr);

    // Build a local WRITE request (LOCAL_SEGMENT_ID).
    Request req{};
    req.opcode = Request::WRITE;
    req.source = src_raw;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(dst_raw);
    req.length = kBufLen;

    auto sub_s = transport.submitTransferTasks(batch, {req});
    ASSERT_TRUE(sub_s.ok()) << sub_s.message();

    TransferStatus ts{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, ts).ok());
    EXPECT_EQ(ts.s, COMPLETED);
    EXPECT_EQ(ts.transferred_bytes, kBufLen);
    EXPECT_EQ(std::memcmp(src_raw, dst_raw, kBufLen), 0);

    transport.removeMemoryBuffer(src_desc);
    transport.freeSubBatch(batch);
    free(src_raw);
    free(dst_raw);
}

TEST(UbTentTransportTest, LargeRequestCreatesMultipleNativeSlices) {
    auto cfg = std::make_shared<Config>();
    cfg->set("transports/ub/slice_size", 1024);

    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, cfg);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    const size_t kBufLen = 4096;
    void* src_raw = alignedAlloc(kBufLen);
    void* dst_raw = alignedAlloc(kBufLen);
    ASSERT_NE(src_raw, nullptr);
    ASSERT_NE(dst_raw, nullptr);
    std::memset(src_raw, 0xCD, kBufLen);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());

    Request req{};
    req.opcode = Request::WRITE;
    req.source = src_raw;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(dst_raw);
    req.length = kBufLen;

    ASSERT_TRUE(transport.submitTransferTasks(batch, {req}).ok());
    auto* ub_batch = dynamic_cast<UbTentTransport::UbSubBatch*>(batch);
    ASSERT_NE(ub_batch, nullptr);
    ASSERT_EQ(ub_batch->task_list.size(), 1u);
    EXPECT_EQ(ub_batch->task_list[0]->slices.size(), 4u);

    TransferStatus ts{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, ts).ok());
    EXPECT_EQ(ts.s, COMPLETED);
    EXPECT_EQ(ts.transferred_bytes, kBufLen);

    transport.freeSubBatch(batch);
    free(src_raw);
    free(dst_raw);
}

TEST(UbTentTransportTest, DeviceMaskRestrictsSelectedDevices) {
    auto cfg = std::make_shared<Config>();
    cfg->set("transports/ub/device_name", "ub0,ub1");
    cfg->set("transports/ub/slice_size", 1024);

    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, cfg);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    const size_t kBufLen = 4096;
    void* src_raw = alignedAlloc(kBufLen);
    void* dst_raw = alignedAlloc(kBufLen);
    ASSERT_NE(src_raw, nullptr);
    ASSERT_NE(dst_raw, nullptr);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    batch->device_mask = 1ULL << 1;

    Request req{};
    req.opcode = Request::WRITE;
    req.source = src_raw;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(dst_raw);
    req.length = kBufLen;

    ASSERT_TRUE(transport.submitTransferTasks(batch, {req}).ok());
    auto* ub_batch = dynamic_cast<UbTentTransport::UbSubBatch*>(batch);
    ASSERT_NE(ub_batch, nullptr);
    for (const auto& slice : ub_batch->task_list[0]->slices) {
        EXPECT_EQ(slice->selected_device_id, 1);
    }

    transport.freeSubBatch(batch);
    free(src_raw);
    free(dst_raw);
}

TEST(UbTentTransportTest, PartialSliceFailureAggregatesTaskFailed) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    auto* ub_batch = dynamic_cast<UbTentTransport::UbSubBatch*>(batch);
    ASSERT_NE(ub_batch, nullptr);

    auto task = std::make_shared<UbTentTransport::UbTask>();
    auto ok_slice = std::make_shared<UbTentTransport::UbSlice>();
    ok_slice->status = COMPLETED;
    ok_slice->length = 1024;
    ok_slice->transferred_bytes = 1024;
    auto failed_slice = std::make_shared<UbTentTransport::UbSlice>();
    failed_slice->status = FAILED;
    failed_slice->length = 1024;
    failed_slice->transferred_bytes = 0;
    task->slices.push_back(ok_slice);
    task->slices.push_back(failed_slice);
    ub_batch->task_list.push_back(task);

    TransferStatus ts{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, ts).ok());
    EXPECT_EQ(ts.s, FAILED);
    EXPECT_EQ(ts.transferred_bytes, 1024u);

    transport.freeSubBatch(batch);
}

// Calling uninstall() twice must not crash.
TEST(UbTentTransportTest, DoubleUninstallSafe) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto s = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!s.ok()) GTEST_SKIP() << "install failed: " << s.message();
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
