/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <folly/CppAttributes.h>
#include <folly/chrono/Hardware.h>
#include <atomic>
#include <chrono>
#include <optional>

namespace facebook::velox {

/// Measures the time between construction and destruction with
/// std::chrono::steady_clock and increments a user-supplied counter with the
/// elapsed time in microseconds.
class MicrosecondTimer {
 public:
  explicit MicrosecondTimer(uint64_t* timer) : timer_(timer) {
    start_ = std::chrono::steady_clock::now();
  }

  ~MicrosecondTimer() {
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start_);

    (*timer_) += duration.count();
  }

 private:
  std::chrono::steady_clock::time_point start_;
  uint64_t* timer_;
};

/// Measures the time between construction and destruction with CPU clock
/// counter (rdtsc on X86) and increments a user-supplied counter with the cycle
/// count.
class ClockTimer {
 public:
  explicit ClockTimer(uint64_t& total)
      : total_(&total), start_(folly::hardware_timestamp()) {}

  explicit ClockTimer(std::atomic<uint64_t>& total)
      : atomicTotal_(&total), start_(folly::hardware_timestamp()) {}

  ~ClockTimer() {
    auto elapsed = folly::hardware_timestamp() - start_;
    if (total_) {
      *total_ += elapsed;
    } else {
      *atomicTotal_ += elapsed;
    }
  }

 private:
  uint64_t* total_{nullptr};
  std::atomic<uint64_t>* atomicTotal_{nullptr};
  uint64_t start_;
};

/// Returns the current epoch time in milliseconds.
size_t getCurrentTimeMs();

/// Returns the current epoch time in microseconds.
size_t getCurrentTimeMicro();

#ifndef NDEBUG
// Used to override the current time for testing purposes.
class ScopedTestTime {
 public:
  ScopedTestTime();
  ~ScopedTestTime();

  void setCurrentTestTimeMs(size_t currentTimeMs);
  void setCurrentTestTimeMicro(size_t currentTimeUs);

  static std::optional<size_t> getCurrentTestTimeMs();
  static std::optional<size_t> getCurrentTestTimeMicro();

 private:
  // Used to verify only one instance of ScopedTestTime exists at a time.
  static bool enabled_;
  // The overridden value of current time only.
  static std::optional<size_t> testTimeUs_;
};
#endif

} // namespace facebook::velox
