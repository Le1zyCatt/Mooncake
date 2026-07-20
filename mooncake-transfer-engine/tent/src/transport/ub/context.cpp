// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/context.h"

#include <algorithm>
#include <limits>

namespace mooncake::tent::ub {

UbContext::UbContext(Topology::NicID topology_id, DeviceInfo device,
                     std::shared_ptr<UrmaAdapter> adapter)
    : topology_id_(topology_id),
      device_(std::move(device)),
      adapter_(std::move(adapter)) {}

UbContext::~UbContext() { (void)shutdown(); }

Status UbContext::initialize(uint32_t jfc_count, const JfcOptions& options) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != State::kUninitialized) {
        return Status::InvalidArgument(
            "UB context can only be initialized once" LOC_MARK);
    }
    if (!adapter_ || !device_.active || jfc_count == 0) {
        return Status::InvalidArgument(
            "Invalid UB context device or JFC count" LOC_MARK);
    }
    // Each UbJfc owns one send JFC and one receive JFC/JFR.
    if (device_.capabilities.max_jfc != 0 &&
        (static_cast<uint64_t>(jfc_count) * 2U) >
            device_.capabilities.max_jfc) {
        return Status::InvalidArgument(
            "Requested UB JFC count exceeds device capability" LOC_MARK);
    }

    auto status = adapter_->openContext(device_, handle_);
    if (!status.ok()) return status;

    jfcs_.reserve(jfc_count);
    for (uint32_t i = 0; i < jfc_count; ++i) {
        JfcPtr native_jfc;
        status = adapter_->createJfc(handle_, options, native_jfc);
        if (!status.ok()) {
            for (auto it = jfcs_.rbegin(); it != jfcs_.rend(); ++it) {
                (void)(*it)->close();
            }
            jfcs_.clear();
            (void)adapter_->closeContext(handle_);
            return status;
        }
        jfcs_.push_back(
            std::make_shared<UbJfc>(i, adapter_, std::move(native_jfc)));
    }
    state_.store(State::kActive, std::memory_order_release);
    return Status::OK();
}

Status UbContext::shutdown() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    auto current = state_.load(std::memory_order_relaxed);
    if (current == State::kClosed || current == State::kUninitialized) {
        if (current == State::kUninitialized) {
            state_.store(State::kClosed, std::memory_order_release);
        }
        return Status::OK();
    }
    state_.store(State::kDraining, std::memory_order_release);

    Status first_error = Status::OK();
    for (auto it = jfcs_.rbegin(); it != jfcs_.rend(); ++it) {
        auto status = (*it)->close();
        if (!status.ok() && first_error.ok()) first_error = status;
    }
    jfcs_.clear();
    auto status = adapter_->closeContext(handle_);
    if (!status.ok() && first_error.ok()) first_error = status;
    state_.store(State::kClosed, std::memory_order_release);
    return first_error;
}

std::shared_ptr<UbJfc> UbContext::jfc(size_t index) const {
    if (jfcs_.empty()) return nullptr;
    return jfcs_[index % jfcs_.size()];
}

void UbContext::addInflight(uint64_t bytes) noexcept {
    inflight_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    outstanding_wrs_.fetch_add(1, std::memory_order_relaxed);
}

void UbContext::markUnavailable() noexcept {
    State expected = State::kActive;
    (void)state_.compare_exchange_strong(expected, State::kFailed,
                                         std::memory_order_acq_rel);
}

void UbContext::removeInflight(uint64_t bytes) noexcept {
    auto current = inflight_bytes_.load(std::memory_order_relaxed);
    while (!inflight_bytes_.compare_exchange_weak(
        current, current >= bytes ? current - bytes : 0,
        std::memory_order_relaxed)) {
    }
    current = outstanding_wrs_.load(std::memory_order_relaxed);
    while (current != 0 &&
           !outstanding_wrs_.compare_exchange_weak(current, current - 1,
                                                   std::memory_order_relaxed)) {
    }
}

}  // namespace mooncake::tent::ub
