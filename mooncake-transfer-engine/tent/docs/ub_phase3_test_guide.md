# TENT UB Transport Phase 3 Test Guide

This guide documents how to validate UB transport support in TENT on the
`ub-tent-native-migration-plan` branch.

This branch provides a TENT-native UB execution path using native UB
primitives. It supports UB-internal chunking and UB-internal device selection,
but cross-transport slice partitioning remains a TENT runtime scheduler
responsibility.

## 1. Scope

`UB_TENT` was the compatibility milestone: it made UB visible to TENT, but the
data path still adapted a TENT request into classic TE `TransferTask` work.

`ub-tent-native-migration-plan` is the native migration branch: TENT selects UB,
then `UbTentTransport` builds native UB tasks/slices, parses TENT UB metadata
directly, and submits primitive UB work through `UbNativePrimitive`.

| Area                     | `UB_TENT` compatibility path                         | `ub-tent-native-migration-plan`                                  |
| ------------------------ | ---------------------------------------------------- | ----------------------------------------------------------------- |
| TENT transport enum      | Adds `TransportType::UB`                             | Keeps `TransportType::UB`                                         |
| Transport selector       | Supports `"ub"` as a policy entry                    | Keeps policy/priority/hint selection in TENT runtime              |
| Transport loader         | Creates `UbTentTransport` when UB is enabled         | Creates the same backend, now with native UB execution            |
| TENT UB backend          | Adapter over classic TE UB task/batch execution      | Native `UbTask`/`UbSlice` execution owned by `UbTentTransport`    |
| Primitive layer          | Classic UB owns request slicing and submit           | `UbNativePrimitive` wraps URMA register/endpoint/post/poll only   |
| UB bootstrap             | Uses the old UB metadata bridge                      | Consumes TENT UB device attrs for endpoint setup                  |
| Local segment publishing | Publishes UB attrs for adapter compatibility         | Publishes UB EID, jetty, tseg, and l_seg_index through TENT attrs |
| Submit fallback          | Adapter submit failures are reported at batch level  | Native slice/task failures are surfaced for runtime failover      |
| URMA registration        | Classic UB owns registration lifetime                | TENT UB backend tracks registration refcount and in-flight use    |
| Tests                    | Basic UB enablement checks                           | Native selector, metadata, chunking, mask, and status tests       |

Main files to review:

```text
mooncake-transfer-engine/tent/include/tent/transport/ub/*
mooncake-transfer-engine/tent/src/transport/ub/*
mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h
mooncake-transfer-engine/tent/src/runtime/control_plane.cpp
mooncake-transfer-engine/tent/include/tent/rpc/rpc.h
mooncake-transfer-engine/tent/src/runtime/transport_selector.cpp
mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp
mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp
mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp
mooncake-transfer-engine/tent/tests/ub_e2e_dual_node_test.cpp
mooncake-transfer-engine/tent/tests/CMakeLists.txt
mooncake-transfer-engine/src/transport/kunpeng_transport/ub_transport.cpp
mooncake-transfer-engine/src/transport/kunpeng_transport/urma/urma_endpoint.cpp
mooncake-transfer-engine/include/transport/kunpeng_transport/ub_context.h
mooncake-transfer-engine/include/transport/kunpeng_transport/urma/urma_endpoint.h
```

## 2. Build Configuration

Use `BUILD_UNIT_TESTS`, not `BUILD_TESTS`.

There is no `MOCK_URMA` CMake option in the current code. Mock URMA is selected by the existing UB CMake logic when the real URMA library is not found.

Typical local build:

```bash
cmake -S . -B build-ub-tent \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug

cmake --build build-ub-tent \
  --target tent_xport_ub tent_ub_transport_test tent_ub_e2e_dual_node_test \
  --parallel
```

If URMA headers are installed in a non-default location:

```bash
cmake -S . -B build-ub-tent \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DURMA_INCLUDE_DIR=/path/to/urma/include \
  -DCMAKE_BUILD_TYPE=Debug
```

If the real URMA runtime is available on the test machine, make sure the runtime library and the selected UB device are usable before running hardware tests.

## 3. Local Unit Test

Unit test source:

```text
mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp
```

CTest command:

```bash
ctest --test-dir build-ub-tent \
  -R '^tent_ub_transport_test$' \
  --output-on-failure \
  -V
```

Direct binary command:

```bash
cd build-ub-tent

GLOG_logtostderr=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_transport_test
```

Run one GTest case:

```bash
cd build-ub-tent

GLOG_logtostderr=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_transport_test \
  --gtest_filter="UbTentTransportTest.InstallWithMockUrma"
```

## 4. Unit Test Coverage

| Test case                                         | Coverage                                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `UbSelectorTest.TypeNameRoundTrip`                | Verifies that `TransportSelector::transportTypeName(UB)` returns `"ub"`                  |
| `UbSelectorTest.ParseUbString`                    | Verifies that `"ub"` parses to `TransportType::UB`                                       |
| `UbSelectorTest.ParseUnknownStillReturnsUnspec`   | Verifies that unknown transport strings still return `UNSPEC`                            |
| `UbSelectorTest.UbEnumValue`                      | Verifies that the UB enum value is inside the supported transport range                  |
| `UbSelectorTest.SelectorPolicyForceUbWithTcpAlsoEnabled` | Verifies that policy `transports=["ub"]` selects UB even when TCP is enabled      |
| `UbSelectorTest.SelectorPolicyForceTcpDoesNotUseUb`      | Verifies that policy `transports=["tcp"]` does not use UB                         |
| `UbSelectorTest.SelectorPolicyUbRdmaTcpPriority`         | Verifies UB/RDMA/TCP policy order and failover index behavior                     |
| `UbSelectorTest.SelectorHintUbChoosesUb`                 | Verifies per-request `transport_hint=ub` can select UB                            |
| `UbSelectorTest.SelectorHintTcpDoesNotUseUb`             | Verifies `transport_hint=tcp` does not accidentally route to UB                   |
| `UbMetadataAttrsTest.ParseBufferAttrsRequiresTsegAndLocalSegmentIndex` | Verifies strict UB buffer attr parsing                         |
| `UbMetadataAttrsTest.ParseDeviceAttrsRequiresEndpointJettyAndDeviceId` | Verifies strict UB device attr parsing                         |
| `UbTentTransportTest.InstallWithMockUrma`         | Verifies basic install lifecycle with mock URMA and checks the transport name/capability |
| `UbTentTransportTest.AddAndRemoveMemoryBuffer`    | Verifies page-aligned memory add/remove flow and updates to `desc.transports`            |
| `UbTentTransportTest.RepeatedRegisterUnregisterSameVA` | Verifies duplicate registration refcount behavior                                      |
| `UbTentTransportTest.AllocateAndFreeSubBatch`     | Verifies native sub-batch allocation and release                                         |
| `UbTentTransportTest.SubmitAndPollMockTransfer`   | Verifies local native chunk status aggregation                                           |
| `UbTentTransportTest.LargeRequestCreatesMultipleNativeSlices` | Verifies UB-internal chunking for large requests                               |
| `UbTentTransportTest.RemoteMetadataFailureCreatesFailedTrackedSlice` | Verifies metadata failure produces a tracked failed slice                    |
| `UbTentTransportTest.DeviceMaskRestrictsSelectedDevices` | Verifies selector device mask is respected by UB device selection                    |
| `UbTentTransportTest.PartialSliceFailureAggregatesTaskFailed` | Verifies any failed native slice fails the logical task                         |
| `UbTentTransportTest.DoubleUninstallSafe`         | Verifies that repeated uninstall is safe                                                 |

The unit test mainly covers selector mapping, native backend lifecycle, memory buffer lifecycle, sub-batch lifecycle, and mock transfer smoke behavior.

It does not prove that the real UB data plane completes on hardware. It also
does not fully simulate a long-running asynchronous remote UB post; that deeper
validation belongs to the dual-node integration test and hardware inspection.

On a host where real `liburma.so` is present but no usable UB HCA exists, hardware-dependent setup may fail and the corresponding test path may skip. That is expected for local developer machines without Kunpeng UB hardware.

## 5. Behavior To Validate

### 5.1 Transport Selection

The branch adds `UB` to the TENT transport enum and maps it to `"ub"`.

Example policy:

```json
{
  "policy": [
    {
      "name": "kunpeng_ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub", "rdma", "tcp"]
    }
  ]
}
```

Expected behavior:

```text
The selector can parse "ub" and select TransportType::UB when UB is available.
```

### 5.2 UB Transport Loading

When built with both `USE_UB=ON` and `USE_TENT=ON`, TENT can load UB if UB is enabled in config.

Example:

```json
{
  "transports": {
    "ub": {
      "enable": true
    }
  }
}
```

Disable UB explicitly:

```json
{
  "transports": {
    "ub": {
      "enable": false
    }
  }
}
```

### 5.3 Device Selection

`UbTentTransport::install()` should resolve UB devices in this order:

```text
1. TENT config key: transports/ub/device_name
2. Environment variable: MC_UB_DEVICE_NAME
3. Auto-discovery of UB devices
```

For Kunpeng SuperNode environments, pin the expected logical or bonded UB device explicitly.

Example config:

```json
{
  "transports": {
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  }
}
```

Equivalent environment override:

```bash
export MC_UB_DEVICE_NAME=bonding_dev_0
```

`device_name` and `MC_UB_DEVICE_NAME` may be comma-separated lists if multiple UB devices should be considered.

### 5.4 Local Segment Publishing

After UB transport installation, the local TENT segment should publish UB-specific device information.

Expected behavior:

```text
MemorySegmentDesc.devices[*].transport_attrs[UB] contains UB EID information.
MemorySegmentDesc.transport_attrs[UB] marks UB availability.
BufferDesc.transport_attrs[UB] contains the UB tseg handle after memory registration.
BufferDesc.transports contains TransportType::UB after successful registration.
```

This is needed because the remote node consumes TENT `SegmentDesc` metadata
directly when building native UB primitive work requests.

### 5.5 Native Primitive Boundary

`UbNativePrimitive` is the only layer that touches the existing Kunpeng UB/URMA
implementation. It exposes memory registration, remote tseg import, endpoint
setup, post send, and completion callbacks. It does not expose old TE
classic TE task or batch semantics to `UbTentTransport`.

Expected behavior:

```text
UbTentTransport parses remote TENT SegmentDesc.transport_attrs[UB].
UbTentTransport performs UB-internal chunking only after TENT selects UB.
UbNativePrimitive submits native UB primitive slices.
Completion callbacks update native UbSlice/UbTask state.
```

### 5.6 UB Endpoint Setup

The native path publishes UB EID and jetty information in TENT device attrs.
Endpoint setup consumes these attrs directly through the UB primitive layer.

Expected call path:

```text
TENT selector chooses UB
  -> UbTentTransport creates UB-internal chunks
  -> UbTentTransport resolves remote SegmentDesc.transport_attrs[UB]
  -> UbNativePrimitive sets up the UB endpoint with remote EID/jetty attrs
  -> UB primitive post/poll completes native UbSlice state
```

`UbBootstrapDesc` should carry:

```text
local_nic_path
peer_nic_path
jetty_num
local_eid
reply_msg
```

This validates that UB connection setup can use the TENT control plane instead of the old standalone UB handshake daemon.

## 6. Dual-Node Integration Test

Integration test source:

```text
mooncake-transfer-engine/tent/tests/ub_e2e_dual_node_test.cpp
```

Build target:

```bash
cmake --build build-ub-tent \
  --target tent_ub_e2e_dual_node_test \
  --parallel
```

This executable is intentionally not registered with CTest because it requires:

```text
1. Two Kunpeng UB-capable nodes
2. A usable URMA runtime
3. A shared TENT metadata backend
4. Network reachability between both TENT RPC servers
5. UB/URMA fabric connectivity between the two nodes
```

### 6.1 Hardware And Runtime Checks

Run on both nodes:

```bash
ls /dev/urma* || true
urma_cmd -q all
```

If needed, load the platform driver:

```bash
modprobe urma_udrv
```

Also confirm that both nodes can reach:

```text
The shared metadata backend, for example etcd
Each other's TENT RPC server address
The selected UB device, for example bonding_dev_0
```
### 6.2 Start A Shared etcd Metadata Backend

The dual-node test requires both nodes to use the same TENT metadata backend. If `metadata_type` is set to `etcd`, start one etcd instance on either Node A or a third reachable node before running the test.

Example on Node A:

```bash
etcd \
  --name mooncake-ub-test \
  --data-dir /tmp/mooncake-ub-etcd \
  --listen-client-urls http://0.0.0.0:11451 \
  --advertise-client-urls http://NODE_A_IP:11451 \
  --listen-peer-urls http://127.0.0.1:11452 \
  --initial-advertise-peer-urls http://127.0.0.1:11452 \
  --initial-cluster mooncake-ub-test=http://127.0.0.1:11452 \
  --initial-cluster-state new
```

Both Node A and Node B should use the same metadata server address:

```json
"metadata_type": "etcd",
"metadata_servers": "NODE_A_IP:11451"
```

Do not use `127.0.0.1:11451` in the config unless both the server and client run on the same host. In a two-node test, `127.0.0.1` on Node B points to Node B itself, not to the etcd instance on Node A.

Before running the test, verify connectivity from both nodes:

```bash
curl http://NODE_A_IP:11451/version
```

### 6.3 Recommended Config Files

The test binary has a built-in UB-only config, but real two-node testing should use explicit config files.

Server config example, `node_a_ub.json`:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_a_seg",
  "rpc_server_hostname": "NODE_A_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": {
      "enable": false
    },
    "rdma": {
      "enable": false
    },
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  },
  "policy": [
    {
      "name": "ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub"]
    }
  ]
}
```

Client config example, `node_b_ub.json`:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_b_seg",
  "rpc_server_hostname": "NODE_B_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": {
      "enable": false
    },
    "rdma": {
      "enable": false
    },
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  },
  "policy": [
    {
      "name": "ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub"]
    }
  ]
}
```

Notes:

```text
1. Use the same metadata backend on both nodes.
2. Use different local segment names on the two nodes.
3. Keep the server's --segment_name consistent with local_segment_name to avoid confusion.
4. If metadata_type is p2p, TENT may replace the local segment name with the RPC address.
5. For stable cross-node tests, etcd is easier to reason about than p2p metadata.
```

### 6.4 Run The Test

On Node A:

```bash
cd build-ub-tent

GLOG_logtostderr=1 GLOG_v=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=server \
  --segment_name=node_a_seg \
  --transport_config=/path/to/node_a_ub.json
```

On Node B:

```bash
cd build-ub-tent

GLOG_logtostderr=1 GLOG_v=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=client \
  --remote_segment=node_a_seg \
  --transport_config=/path/to/node_b_ub.json \
  --data_size=1048576 \
  --operation=write
```

Expected client-side result:

```text
Client: WRITE 1048576 bytes ... COMPLETED
Client: READ 1048576 bytes ... COMPLETED
Client: data integrity VERIFIED
Client: test PASSED
```

`--operation=write` writes a known pattern to the remote buffer, reads it back, and verifies the returned bytes.

`--operation=read` only executes the read path and does not verify a known pattern.

### 6.5 What The Integration Test Covers

The dual-node integration test validates:

```text
1. Server-side TENT segment publication
2. UB EID publication through TENT segment device attrs
3. UB tseg publication through TENT buffer attrs
4. Client-side remote segment open
5. Native remote metadata parsing through TENT SegmentDesc attrs
6. UB-internal chunk creation through UbTentTransport
7. UB endpoint setup through native UB device attrs
8. Native UB primitive post/poll completion callbacks
9. Remote WRITE
10. Remote READ
11. Data integrity after write + read-back
```

## 7. Debugging Integration Failures

| Symptom                            | Checks                                                                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `openSegment("node_a_seg") failed` | Confirm both nodes use the same metadata backend, the server is still running, and `local_segment_name` is `node_a_seg`         |
| UB transport not installed         | Check `USE_UB=ON`, `USE_TENT=ON`, `transports.ub.enable=true`, URMA headers/runtime, and `device_name`                          |
| Wrong UB device selected           | Set `transports.ub.device_name` or `MC_UB_DEVICE_NAME` explicitly                                                               |
| Remote segment has no buffers      | Confirm server-side memory registration succeeded and segment synchronization reached metadata                                  |
| Remote segment has no UB attrs     | Confirm UB transport installation happened before segment synchronization                                                       |
| UB endpoint setup failed           | Check published UB EID/jetty attrs, fabric reachability, and endpoint setup logs                                                |
| URMA duplicate registration error  | Confirm the branch includes the primary-register plus adopt-segment lifecycle change                                            |
| Transfer hangs                     | Check UB fabric connectivity, selected UB device, endpoint creation logs, and poll/fallback logs                                |
| Data mismatch                      | Use `--operation=write`, confirm both nodes use the same `data_size`, and make sure no unintended fallback transport is enabled |

Useful etcd inspection:

```bash
etcdctl get mooncake/tent/ --prefix

etcdctl get mooncake/tent/node_a_seg --print-value-only | python3 -m json.tool
```

Look for:

```text
detail.devices[*].transport_attrs containing UB EID information
detail.buffers[*].transport_attrs containing UB tseg information
rpc_server_addr pointing to Node A's reachable TENT RPC address
```

## 8. Fallback And Regression Tests

This branch also changes submit failure handling so that a failed sub-batch submit can be retried through poll-time failover.

Run existing TENT regression tests:

```bash
ctest --test-dir build-ub-tent \
  -R '^tent_engine_failover_e2e_test$' \
  --output-on-failure \
  -V

ctest --test-dir build-ub-tent \
  -R '^tent_transport_hint_test$' \
  --output-on-failure \
  -V

ctest --test-dir build-ub-tent \
  -R '^tent_transport_selector_test$' \
  --output-on-failure \
  -V
```

Old Transfer Engine UB regression target:

```bash
cmake --build build-ub-tent \
  --target ub_transport_test \
  --parallel
```

Example run:

```bash
GLOG_logtostderr=1 \
build-ub-tent/mooncake-transfer-engine/tests/ub_transport_test \
  --device_name=mock_urma_device
```

`ub_transport_test` may allocate a large NUMA buffer, so it may not be suitable for every developer machine.

## 9. Test Matrix

| Test                            |                                 Hardware | CTest | Coverage                                                                                                                        |
| ------------------------------- | ---------------------------------------: | ----: | ------------------------------------------------------------------------------------------------------------------------------- |
| `tent_ub_transport_test`        | No real UB hardware if mock URMA is used |   Yes | UB enum/string mapping, selector, install lifecycle, memory registration lifecycle, sub-batch lifecycle, mock submit/poll smoke |
| `tent_ub_e2e_dual_node_test`    |        Yes, two Kunpeng UB-capable nodes |    No | TENT segment publishing, native UB metadata parsing, UB primitive post/poll, data integrity                                    |
| `tent_engine_failover_e2e_test` |                                       No |   Yes | Submit-failure poll-time resubmit behavior                                                                                      |
| `tent_transport_hint_test`      |                                       No |   Yes | Transport hint behavior after adding UB                                                                                         |
| `tent_transport_selector_test`  |                                       No |   Yes | Selector regression around transport policy handling                                                                            |
| `ub_transport_test`             |      Mock or real UB, depending on build |    No | Old Transfer Engine UB registration and transfer regression                                                                     |

## 10. Common Pitfalls

Do not use:

```bash
-DMOCK_URMA=ON
```

The current code does not define this CMake option.

Do not use:

```bash
-DBUILD_TESTS=ON
```

Use:

```bash
-DBUILD_UNIT_TESTS=ON
```

Use the real TENT UB targets:

```text
tent_ub_transport_test
tent_ub_e2e_dual_node_test
```

Do not use old or planned names such as:

```text
tent_ub_transfer_test
tent_ub_e2e_test
```

The dual-node executable is not registered as a CTest test.

For dual-node tests, prefer explicit config files and shared metadata. Keep the server `--segment_name` aligned with the config `local_segment_name`.

UB memory registration should use page-aligned buffers. The tests use page-aligned allocation for this reason.

## 11. Expected Validation Summary

A complete validation should include:

```text
1. Build with USE_TENT=ON and USE_UB=ON.
2. Run tent_ub_transport_test locally.
3. Run selector, hint, and failover regression tests.
4. Run old Transfer Engine ub_transport_test when the environment allows it.
5. Run tent_ub_e2e_dual_node_test on two Kunpeng UB-capable nodes.
6. Confirm metadata contains UB EID and tseg attrs.
7. Confirm native UB endpoint setup consumes published EID/jetty attrs.
8. Confirm write + read-back data integrity in the dual-node test.
```
