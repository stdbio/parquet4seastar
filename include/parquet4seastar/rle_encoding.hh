// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Imported from Apache Impala (incubating) on 2016-01-29 and modified for use
// in parquet-cpp, Arrow

#pragma once

#include <algorithm>
#include <cmath>
#include <vector>

#include <parquet4seastar/bit_stream_utils.hh>

namespace parquet4seastar {

using BitReader = BitUtil::BitReader;

/// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
/// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
/// (literal encoding).
/// For both types of runs, there is a byte-aligned indicator which encodes the length
/// of the run and the type of the run.
/// This encoding has the benefit that when there aren't any long enough runs, values
/// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
/// the run length are byte aligned. This allows for very efficient decoding
/// implementations.
/// The encoding is:
///    encoded-block := run*
///    run := literal-run | repeated-run
///    literal-run := literal-indicator < literal bytes >
///    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
///    literal-indicator := varint_encode( number_of_groups << 1 | 1)
///    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
/// Each run is preceded by a varint. The varint's least significant bit is
/// used to indicate whether the run is a literal run or a repeated run. The rest
/// of the varint is used to determine the length of the run (eg how many times the
/// value repeats).
//
/// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
/// in groups of 8), so that no matter the bit-width of the value, the sequence will end
/// on a byte boundary without padding.
/// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
/// the actual number of encoded ints. (This means that the total number of encoded values
/// can not be determined from the encoded data, since the number of values in the last
/// group may not be a multiple of 8). For the last group of literal runs, we pad
/// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
/// without the need for additional checks.
//
/// There is a break-even point when it is more storage efficient to do run length
/// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
/// for both the repeated encoding or the literal encoding.  This value can always
/// be computed based on the bit-width.
/// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
/// Examples with bit-width 1 (eg encoding booleans):
/// ----------------------------------------
/// 100 1s followed by 100 0s:
/// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
///  - (total 4 bytes)
//
/// alternating 1s and 0s (200 total):
/// 200 ints = 25 groups of 8
/// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
/// (total 26 bytes, 1 byte overhead)

namespace BitUtil {

// Returns the ceil of value/divisor
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
  return (value == 0) ? 0 : 1 + (value - 1) / divisor;
}

} // namespace BitUtil

/// Decoder class for RLE encoded data.
class RleDecoder {
 public:
  /// Create a decoder object. buffer/buffer_len is the decoded data.
  /// bit_width is the width of each value (before encoding).
  RleDecoder(const uint8_t* buffer, int buffer_len, int bit_width)
      : bit_reader_(buffer, buffer_len),
        bit_width_(bit_width),
        current_value_(0),
        repeat_count_(0),
        literal_count_(0) {
    assert(bit_width_ >= 0);
    assert(bit_width_ <= 64);
  }

  RleDecoder() : bit_width_(-1) {}

  void Reset(const uint8_t* buffer, int buffer_len, int bit_width) {
    assert(bit_width >= 0);
    assert(bit_width <= 64);
    bit_reader_.Reset(buffer, buffer_len);
    bit_width_ = bit_width;
    current_value_ = 0;
    repeat_count_ = 0;
    literal_count_ = 0;
  }

  /// Gets the next value.  Returns false if there are no more.
  template <typename T>
  bool Get(T* val);

  /// Gets a batch of values.  Returns the number of decoded elements.
  template <typename T>
  int GetBatch(T* values, int batch_size);

 protected:
  BitUtil::BitReader bit_reader_;
  /// Number of bits needed to encode the value. Must be between 0 and 64.
  int bit_width_;
  uint64_t current_value_;
  int32_t repeat_count_;
  int32_t literal_count_;

 private:
  /// Fills literal_count_ and repeat_count_ with next values. Returns false if there
  /// are no more.
  template <typename T>
  bool NextCounts();
};

/// Class to incrementally build the rle data.   This class does not allocate any memory.
/// The encoding has two modes: encoding repeated runs and literal runs.
/// If the run is sufficiently short, it is more efficient to encode as a literal run.
/// This class does so by buffering 8 values at a time.  If they are not all the same
/// they are added to the literal run.  If they are the same, they are added to the
/// repeated run.  When we switch modes, the previous run is flushed out.
class RleEncoder {
 public:
  /// buffer/buffer_len: preallocated output buffer.
  /// bit_width: max number of bits for value.
  /// TODO: consider adding a min_repeated_run_length so the caller can control
  /// when values should be encoded as repeated runs.  Currently this is derived
  /// based on the bit_width, which can determine a storage optimal choice.
  /// TODO: allow 0 bit_width (and have dict encoder use it)
  RleEncoder(uint8_t* buffer, int buffer_len, int bit_width)
      : bit_width_(bit_width), bit_writer_(buffer, buffer_len) {
    assert(bit_width_ >= 0);
    assert(bit_width_ <= 64);
    max_run_byte_size_ = MinBufferSize(bit_width);
    assert(buffer_len >= max_run_byte_size_ && "Input buffer not big enough.");
    Clear();
  }

  /// Returns the minimum buffer size needed to use the encoder for 'bit_width'
  /// This is the maximum length of a single run for 'bit_width'.
  /// It is not valid to pass a buffer less than this length.
  static int MinBufferSize(int bit_width) {
    /// 1 indicator byte and MAX_VALUES_PER_LITERAL_RUN 'bit_width' values.
    int max_literal_run_size =
        1 +
        static_cast<int>(BitUtil::BytesForBits(MAX_VALUES_PER_LITERAL_RUN * bit_width));
    /// Up to kMaxVlqByteLength indicator and a single 'bit_width' value.
    int max_repeated_run_size
        = 5 // Max 32 bit VLQ size
        + static_cast<int>(BitUtil::BytesForBits(bit_width));
    return std::max(max_literal_run_size, max_repeated_run_size);
  }

  /// Returns the maximum byte size it could take to encode 'num_values'.
  static int MaxBufferSize(int bit_width, int num_values) {
    // For a bit_width > 1, the worst case is the repetition of "literal run of length 8
    // and then a repeated run of length 8".
    // 8 values per smallest run, 8 bits per byte
    int bytes_per_run = bit_width;
    int num_runs = static_cast<int>(BitUtil::CeilDiv(num_values, 8));
    int literal_max_size = num_runs + num_runs * bytes_per_run;

    // In the very worst case scenario, the data is a concatenation of repeated
    // runs of 8 values. Repeated run has a 1 byte varint followed by the
    // bit-packed repeated value
    int min_repeated_run_size = 1 + static_cast<int>(BitUtil::BytesForBits(bit_width));
    int repeated_max_size =
        static_cast<int>(BitUtil::CeilDiv(num_values, 8)) * min_repeated_run_size;

    return std::max(literal_max_size, repeated_max_size);
  }

  /// Encode value.  Returns true if the value fits in buffer, false otherwise.
  /// This value must be representable with bit_width_ bits.
  bool Put(uint64_t value);

  /// Flushes any pending values to the underlying buffer.
  /// Returns the total number of bytes written
  int Flush();

  /// Resets all the state in the encoder.
  void Clear();

  /// Returns pointer to underlying buffer
  uint8_t* buffer() { return bit_writer_.buffer(); }
  int32_t len() { return bit_writer_.bytes_written(); }

 private:
  /// Flushes any buffered values.  If this is part of a repeated run, this is largely
  /// a no-op.
  /// If it is part of a literal run, this will call FlushLiteralRun, which writes
  /// out the buffered literal values.
  /// If 'done' is true, the current run would be written even if it would normally
  /// have been buffered more.  This should only be called at the end, when the
  /// encoder has received all values even if it would normally continue to be
  /// buffered.
  void FlushBufferedValues(bool done);

  /// Flushes literal values to the underlying buffer.  If update_indicator_byte,
  /// then the current literal run is complete and the indicator byte is updated.
  void FlushLiteralRun(bool update_indicator_byte);

  /// Flushes a repeated run to the underlying buffer.
  void FlushRepeatedRun();

  /// Checks and sets buffer_full_. This must be called after flushing a run to
  /// make sure there are enough bytes remaining to encode the next run.
  void CheckBufferFull();

  /// The maximum number of values in a single literal run
  /// (number of groups encodable by a 1-byte indicator * 8)
  static const int MAX_VALUES_PER_LITERAL_RUN = (1 << 6) * 8;

  /// Number of bits needed to encode the value. Must be between 0 and 64.
  int bit_width_;

  /// Underlying buffer.
  BitUtil::BitWriter bit_writer_;

  /// If true, the buffer is full and subsequent Put()'s will fail.
  bool buffer_full_;

  /// The maximum byte size a single run can take.
  int max_run_byte_size_;

  /// We need to buffer at most 8 values for literals.  This happens when the
  /// bit_width is 1 (so 8 values fit in one byte).
  /// TODO: generalize this to other bit widths
  int64_t buffered_values_[8];

  /// Number of values in buffered_values_
  int num_buffered_values_;

  /// The current (also last) value that was written and the count of how
  /// many times in a row that value has been seen.  This is maintained even
  /// if we are in a literal run.  If the repeat_count_ get high enough, we switch
  /// to encoding repeated runs.
  uint64_t current_value_;
  int repeat_count_;

  /// Number of literals in the current run.  This does not include the literals
  /// that might be in buffered_values_.  Only after we've got a group big enough
  /// can we decide if they should part of the literal_count_ or repeat_count_
  int literal_count_;

  /// Pointer to a byte in the underlying buffer that stores the indicator byte.
  /// This is reserved as soon as we need a literal run but the value is written
  /// when the literal run is complete.
  uint8_t* literal_indicator_byte_;
};

template <typename T>
inline bool RleDecoder::Get(T* val) {
  return GetBatch(val, 1) == 1;
}

template <typename T>
inline int RleDecoder::GetBatch(T* values, int batch_size) {
  assert(bit_width_ >= 0);
  int values_read = 0;

  auto* out = values;

  while (values_read < batch_size) {
    int remaining = batch_size - values_read;

    if (repeat_count_ > 0) {
      int repeat_batch = std::min(remaining, repeat_count_);
      std::fill(out, out + repeat_batch, static_cast<T>(current_value_));

      repeat_count_ -= repeat_batch;
      values_read += repeat_batch;
      out += repeat_batch;
    } else if (literal_count_ > 0) {
      int literal_batch = std::min(remaining, literal_count_);
      int actual_read = bit_reader_.GetBatch(bit_width_, out, literal_batch);
      if (actual_read != literal_batch) {
        return values_read;
      }

      literal_count_ -= literal_batch;
      values_read += literal_batch;
      out += literal_batch;
    } else {
      if (!NextCounts<T>()) return values_read;
    }
  }

  return values_read;
}

static inline bool IndexInRange(int32_t idx, int32_t dictionary_length) {
  return idx >= 0 && idx < dictionary_length;
}

template <typename T>
bool RleDecoder::NextCounts() {
  // Read the next run's indicator int, it could be a literal or repeated run.
  // The int is encoded as a vlq-encoded value.
  uint32_t indicator_value = 0;
  if (!bit_reader_.GetVlqInt(&indicator_value)) return false;

  // lsb indicates if it is a literal run or repeated run
  bool is_literal = indicator_value & 1;
  uint32_t count = indicator_value >> 1;
  if (is_literal) {
    if (__builtin_expect(count > static_cast<uint32_t>(INT32_MAX) / 8, false)) {
      return false;
    }
    literal_count_ = count * 8;
  } else {
    if (__builtin_expect(count > static_cast<uint32_t>(INT32_MAX), false)) {
      return false;
    }
    repeat_count_ = count;
    // XXX (ARROW-4018) this is not big-endian compatible
    if (!bit_reader_.GetAligned<T>(static_cast<int>(BitUtil::CeilDiv(bit_width_, 8)),
                                   reinterpret_cast<T*>(&current_value_))) {
      return false;
    }
  }
  return true;
}

/// This function buffers input values 8 at a time.  After seeing all 8 values,
/// it decides whether they should be encoded as a literal or repeated run.
inline bool RleEncoder::Put(uint64_t value) {
  assert(bit_width_ == 64 || value < (1ULL << bit_width_));
  if (__builtin_expect(buffer_full_, false)) return false;

  if (__builtin_expect(current_value_ == value, true)) {
    ++repeat_count_;
    if (repeat_count_ > 8) {
      // This is just a continuation of the current run, no need to buffer the
      // values.
      // Note that this is the fast path for long repeated runs.
      return true;
    }
  } else {
    if (repeat_count_ >= 8) {
      // We had a run that was long enough but it has ended.  Flush the
      // current repeated run.
      assert(literal_count_ == 0);
      FlushRepeatedRun();
    }
    repeat_count_ = 1;
    current_value_ = value;
  }

  buffered_values_[num_buffered_values_] = value;
  if (++num_buffered_values_ == 8) {
    assert(literal_count_ % 8 == 0);
    FlushBufferedValues(false);
  }
  return true;
}

inline void RleEncoder::FlushLiteralRun(bool update_indicator_byte) {
  if (literal_indicator_byte_ == NULL) {
    // The literal indicator byte has not been reserved yet, get one now.
    literal_indicator_byte_ = bit_writer_.GetNextBytePtr();
    assert(literal_indicator_byte_ != NULL);
  }

  // Write all the buffered values as bit packed literals
  for (int i = 0; i < num_buffered_values_; ++i) {
      [[maybe_unused]] bool success = bit_writer_.PutValue(buffered_values_[i], bit_width_);
      assert(success && "There is a bug in using CheckBufferFull()");
  }
  num_buffered_values_ = 0;

  if (update_indicator_byte) {
    // At this point we need to write the indicator byte for the literal run.
    // We only reserve one byte, to allow for streaming writes of literal values.
    // The logic makes sure we flush literal runs often enough to not overrun
    // the 1 byte.
    assert(literal_count_ % 8 == 0);
    int num_groups = literal_count_ / 8;
    int32_t indicator_value = (num_groups << 1) | 1;
    assert((indicator_value & 0xFFFFFF00) == 0);
    *literal_indicator_byte_ = static_cast<uint8_t>(indicator_value);
    literal_indicator_byte_ = NULL;
    literal_count_ = 0;
    CheckBufferFull();
  }
}

inline void RleEncoder::FlushRepeatedRun() {
  assert(repeat_count_ > 0);
  [[maybe_unused]] bool result = true;
  // The lsb of 0 indicates this is a repeated run
  int32_t indicator_value = repeat_count_ << 1 | 0;
  result &= bit_writer_.PutVlqInt(indicator_value);
  result &= bit_writer_.PutAligned(current_value_,
                                   static_cast<int>(BitUtil::CeilDiv(bit_width_, 8)));
  assert(result);
  num_buffered_values_ = 0;
  repeat_count_ = 0;
  CheckBufferFull();
}

/// Flush the values that have been buffered.  At this point we decide whether
/// we need to switch between the run types or continue the current one.
inline void RleEncoder::FlushBufferedValues(bool done) {
  if (repeat_count_ >= 8) {
    // Clear the buffered values.  They are part of the repeated run now and we
    // don't want to flush them out as literals.
    num_buffered_values_ = 0;
    if (literal_count_ != 0) {
      // There was a current literal run.  All the values in it have been flushed
      // but we still need to update the indicator byte.
      assert(literal_count_ % 8 == 0);
      assert(repeat_count_ == 8);
      FlushLiteralRun(true);
    }
    assert(literal_count_ == 0);
    return;
  }

  literal_count_ += num_buffered_values_;
  assert(literal_count_ % 8 == 0);
  int num_groups = literal_count_ / 8;
  if (num_groups + 1 >= (1 << 6)) {
    // We need to start a new literal run because the indicator byte we've reserved
    // cannot store more values.
    assert(literal_indicator_byte_ != NULL);
    FlushLiteralRun(true);
  } else {
    FlushLiteralRun(done);
  }
  repeat_count_ = 0;
}

inline int RleEncoder::Flush() {
  if (literal_count_ > 0 || repeat_count_ > 0 || num_buffered_values_ > 0) {
    bool all_repeat = literal_count_ == 0 && (repeat_count_ == num_buffered_values_ ||
                                              num_buffered_values_ == 0);
    // There is something pending, figure out if it's a repeated or literal run
    if (repeat_count_ > 0 && all_repeat) {
      FlushRepeatedRun();
    } else {
      assert(literal_count_ % 8 == 0);
      // Buffer the last group of literals to 8 by padding with 0s.
      for (; num_buffered_values_ != 0 && num_buffered_values_ < 8;
           ++num_buffered_values_) {
        buffered_values_[num_buffered_values_] = 0;
      }
      literal_count_ += num_buffered_values_;
      FlushLiteralRun(true);
      repeat_count_ = 0;
    }
  }
  bit_writer_.Flush();
  assert(num_buffered_values_ == 0);
  assert(literal_count_ == 0);
  assert(repeat_count_ == 0);

  return bit_writer_.bytes_written();
}

inline void RleEncoder::CheckBufferFull() {
  int bytes_written = bit_writer_.bytes_written();
  if (bytes_written + max_run_byte_size_ > bit_writer_.buffer_len()) {
    buffer_full_ = true;
  }
}

inline void RleEncoder::Clear() {
  buffer_full_ = false;
  current_value_ = 0;
  repeat_count_ = 0;
  num_buffered_values_ = 0;
  literal_count_ = 0;
  literal_indicator_byte_ = NULL;
  bit_writer_.Clear();
}

}  // namespace parquet4seastar
