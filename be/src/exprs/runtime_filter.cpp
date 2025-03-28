// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/runtime_filter.h"

#include "types/logical_type_infra.h"
#include "util/compression/stream_compression.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// SimdBlockFilter
// ------------------------------------------------------------------------------------

void SimdBlockFilter::init(size_t nums) {
    nums = std::max(MINIMUM_ELEMENT_NUM, nums);
    int log_heap_space = std::ceil(std::log2(nums));
    _log_num_buckets = std::max(1, log_heap_space - LOG_BUCKET_BYTE_SIZE);
    _directory_mask = (1ull << std::min(63, _log_num_buckets)) - 1;
    const size_t alloc_size = get_alloc_size();
    const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&_directory), 64, alloc_size);
    if (malloc_failed) throw ::std::bad_alloc();
    memset(_directory, 0, alloc_size);
}

SimdBlockFilter::SimdBlockFilter(SimdBlockFilter&& bf) noexcept {
    _log_num_buckets = bf._log_num_buckets;
    _directory_mask = bf._directory_mask;
    _directory = bf._directory;
    bf._directory = nullptr;
}

size_t SimdBlockFilter::max_serialized_size() const {
    const size_t alloc_size = _directory == nullptr ? 0 : get_alloc_size();
    return sizeof(_log_num_buckets) + sizeof(_directory_mask) + // data size + max data size
           sizeof(int32_t) + alloc_size;
}

size_t SimdBlockFilter::serialize(uint8_t* data) const {
    size_t offset = 0;
#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    SIMD_BF_COPY_FIELD(_log_num_buckets);
    SIMD_BF_COPY_FIELD(_directory_mask);

    const size_t alloc_size = get_alloc_size();
    int32_t data_size = alloc_size;
    SIMD_BF_COPY_FIELD(data_size);
    if (LIKELY(data_size > 0)) {
        memcpy(data + offset, _directory, data_size);
        offset += data_size;
    }
    return offset;
#undef SIMD_BF_COPY_FIELD
}

size_t SimdBlockFilter::deserialize(const uint8_t* data) {
    size_t offset = 0;
    int32_t data_size = 0;

#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);
    SIMD_BF_COPY_FIELD(_log_num_buckets);
    SIMD_BF_COPY_FIELD(_directory_mask);
    SIMD_BF_COPY_FIELD(data_size);
#undef SIMD_BF_COPY_FIELD
    const size_t alloc_size = get_alloc_size();
    DCHECK(data_size == alloc_size);
    if (LIKELY(data_size > 0)) {
        const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&(_directory)), 64, alloc_size);
        if (malloc_failed) throw ::std::bad_alloc();
        memcpy(_directory, data + offset, data_size);
        offset += data_size;
    }
    return offset;
}

void SimdBlockFilter::merge(const SimdBlockFilter& bf) {
    if (_directory == nullptr || bf._directory == nullptr) {
        return;
    }
    DCHECK(_log_num_buckets == bf._log_num_buckets);
    for (int i = 0; i < (1 << _log_num_buckets); i++) {
#ifdef __AVX2__
        auto* const dst = reinterpret_cast<__m256i*>(_directory[i]);
        auto* const src = reinterpret_cast<__m256i*>(bf._directory[i]);
        const __m256i a = _mm256_load_si256(src);
        const __m256i b = _mm256_load_si256(dst);
        const __m256i c = _mm256_or_si256(a, b);
        _mm256_store_si256(dst, c);
#else
        for (int j = 0; j < BITS_SET_PER_BLOCK; j++) {
            _directory[i][j] |= bf._directory[i][j];
        }
#endif
    }
}

// For scalar version:

void SimdBlockFilter::make_mask(uint32_t key, uint32_t* masks) const {
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        // add some salt to key
        masks[i] = key * SALT[i];
        // masks[i] mod 32
        masks[i] = masks[i] >> 27;
        // set the masks[i]-th bit
        masks[i] = 0x1 << masks[i];
    }
}

bool SimdBlockFilter::check_equal(const SimdBlockFilter& bf) const {
    const size_t alloc_size = get_alloc_size();
    return _log_num_buckets == bf._log_num_buckets && _directory_mask == bf._directory_mask &&
           memcmp(_directory, bf._directory, alloc_size) == 0;
}

void SimdBlockFilter::clear() {
    if (_directory) {
        free(_directory);
        _directory = nullptr;
        _log_num_buckets = 0;
        _directory_mask = 0;
    }
}

// ------------------------------------------------------------------------------------
// RuntimeBloomFilter
// ------------------------------------------------------------------------------------

template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::max_serialized_size() const {
    // todo(yan): noted that it's not serialize compatible with 32-bit and 64-bit.
    auto num_partitions = _hash_partition_bf.size();
    size_t size = sizeof(_has_null) + sizeof(_size) + sizeof(num_partitions) + sizeof(_join_mode);
    if (num_partitions == 0) {
        size += _bf.max_serialized_size();
    } else {
        for (const auto& bf : _hash_partition_bf) {
            size += bf.max_serialized_size();
        }
    }
    return size;
}
template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::serialize(int serialize_version, uint8_t* data) const {
    size_t offset = 0;
    auto num_partitions = _hash_partition_bf.size();

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(num_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.serialize(data + offset);
    } else {
        for (const auto& bf : _hash_partition_bf) {
            offset += bf.serialize(data + offset);
        }
    }
    return offset;
}
template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::deserialize(int serialize_version, const uint8_t* data) {
    size_t offset = 0;
    size_t num_partitions = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(num_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.deserialize(data + offset);
    } else {
        for (size_t i = 0; i < num_partitions; i++) {
            SimdBlockFilter bf;
            offset += bf.deserialize(data + offset);
            _hash_partition_bf.emplace_back(std::move(bf));
        }
    }

    return offset;
}

template <LogicalType Type>
void TRuntimeBloomFilter<Type>::clear_bf() {
    if (_hash_partition_bf.empty()) {
        _bf.clear();
    } else {
        for (size_t i = 0; i < _hash_partition_bf.size(); i++) {
            _hash_partition_bf[i].clear();
        }
    }
    _size = 0;
}

// ------------------------------------------------------------------------------------
// RuntimeEmptyFilter
// ------------------------------------------------------------------------------------

template <LogicalType LT>
std::string RuntimeEmptyFilter<LT>::debug_string() const {
    std::stringstream ss;
    ss << "RuntimeEmptyFilter("
       << "has_null=" << _has_null     //
       << ", join_mode=" << _join_mode //
       << ", num_elements=" << _size   //
       << ")";
    return ss.str();
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::max_serialized_size() const {
    return sizeof(_has_null) + // 1. has_null
           sizeof(_size) +     // 2. num_elements
           sizeof(_join_mode); // 3. join_mode
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::serialize(int serialize_version, uint8_t* data) const {
    DCHECK(serialize_version != RF_VERSION);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

#undef JRF_COPY_FIELD

    return offset;
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::deserialize(int serialize_version, const uint8_t* data) {
    DCHECK(serialize_version != RF_VERSION);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

#undef JRF_COPY_FIELD

    return offset;
}

// ------------------------------------------------------------------------------------
// Instantiate runtime filters.
// ------------------------------------------------------------------------------------

#define InstantiateRuntimeFilter(LT)                                  \
    template class RuntimeEmptyFilter<LT>;                            \
    template class ComposedRuntimeFilter<LT, RuntimeEmptyFilter<LT>>; \
    template class TRuntimeBloomFilter<LT>;                           \
    template class ComposedRuntimeFilter<LT, TRuntimeBloomFilter<LT>>;

APPLY_FOR_ALL_SCALAR_TYPE(InstantiateRuntimeFilter)
#undef InstantiateRuntimeFilter

} // namespace starrocks
