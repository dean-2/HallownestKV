#include "../include/lumafly_bloom.h"
#include <vector>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <algorithm>

static uint32_t murmurhash3_32(const char* key, size_t len, uint32_t seed) {
    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const size_t nblocks = len / 4;
    const uint32_t* blocks = (const uint32_t*)(key);

    for (size_t i = 0; i < nblocks; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> (32 - 15));
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> (32 - 13));
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t* tail = (const uint8_t*)(key + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
    case 3: k1 ^= tail[2] << 16; [[fallthrough]];
    case 2: k1 ^= tail[1] << 8;  [[fallthrough]];
    case 1: k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >> (32 - 15));
            k1 *= c2;
            h1 ^= k1;
    }

    h1 ^= static_cast<uint32_t>(len);
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

struct LumaflyBloom {
    size_t num_bits;
    size_t num_hashes;
    std::vector<uint8_t> bit_array;
};

extern "C" {

LumaflyBloom* lumafly_create(size_t expected_entries, double fp_rate) {
    if (expected_entries == 0) expected_entries = 1;
    if (fp_rate <= 0.0 || fp_rate >= 1.0) fp_rate = 0.001; // Default 0.1% for financial mode

    // Optimal bits m = - (n * ln(p)) / (ln(2)^2)
    double m = - (static_cast<double>(expected_entries) * std::log(fp_rate)) / (std::log(2.0) * std::log(2.0));
    size_t num_bits = static_cast<size_t>(std::ceil(m));
    if (num_bits < 64) num_bits = 64;

    // Optimal hash functions k = (m / n) * ln(2)
    double k = (static_cast<double>(num_bits) / static_cast<double>(expected_entries)) * std::log(2.0);
    size_t num_hashes = static_cast<size_t>(std::ceil(k));
    if (num_hashes < 1) num_hashes = 1;

    size_t num_bytes = (num_bits + 7) / 8;

    LumaflyBloom* bf = new LumaflyBloom();
    bf->num_bits = num_bits;
    bf->num_hashes = num_hashes;
    bf->bit_array.resize(num_bytes, 0);
    return bf;
}

void lumafly_add(LumaflyBloom* bf, const char* key, size_t len) {
    if (!bf || !key) return;

    uint32_t h1 = murmurhash3_32(key, len, 0x9747b28c);
    uint32_t h2 = murmurhash3_32(key, len, h1);

    for (size_t i = 0; i < bf->num_hashes; i++) {
        uint32_t combined_hash = h1 + static_cast<uint32_t>(i) * h2;
        size_t bit_idx = combined_hash % bf->num_bits;
        bf->bit_array[bit_idx / 8] |= (1 << (bit_idx % 8));
    }
}

bool lumafly_contains(const LumaflyBloom* bf, const char* key, size_t len) {
    if (!bf || !key) return false;

    uint32_t h1 = murmurhash3_32(key, len, 0x9747b28c);
    uint32_t h2 = murmurhash3_32(key, len, h1);

    for (size_t i = 0; i < bf->num_hashes; i++) {
        uint32_t combined_hash = h1 + static_cast<uint32_t>(i) * h2;
        size_t bit_idx = combined_hash % bf->num_bits;
        if (!(bf->bit_array[bit_idx / 8] & (1 << (bit_idx % 8)))) {
            return false;
        }
    }
    return true;
}

size_t lumafly_get_serialized_size(const LumaflyBloom* bf) {
    if (!bf) return 0;
    // 8 bytes (num_bits) + 8 bytes (num_hashes) + bit_array bytes
    return sizeof(uint64_t) * 2 + bf->bit_array.size();
}

size_t lumafly_serialize(const LumaflyBloom* bf, char* buf, size_t buf_len) {
    if (!bf || !buf) return 0;

    size_t req_size = lumafly_get_serialized_size(bf);
    if (buf_len < req_size) return 0;

    uint64_t bits = static_cast<uint64_t>(bf->num_bits);
    uint64_t hashes = static_cast<uint64_t>(bf->num_hashes);

    std::memcpy(buf, &bits, sizeof(uint64_t));
    std::memcpy(buf + sizeof(uint64_t), &hashes, sizeof(uint64_t));
    std::memcpy(buf + sizeof(uint64_t) * 2, bf->bit_array.data(), bf->bit_array.size());

    return req_size;
}

LumaflyBloom* lumafly_deserialize(const char* buf, size_t len) {
    if (!buf || len < sizeof(uint64_t) * 2) return nullptr;

    uint64_t bits = 0;
    uint64_t hashes = 0;
    std::memcpy(&bits, buf, sizeof(uint64_t));
    std::memcpy(&hashes, buf + sizeof(uint64_t), sizeof(uint64_t));

    size_t num_bytes = (bits + 7) / 8;
    if (len < sizeof(uint64_t) * 2 + num_bytes) return nullptr;

    LumaflyBloom* bf = new LumaflyBloom();
    bf->num_bits = static_cast<size_t>(bits);
    bf->num_hashes = static_cast<size_t>(hashes);
    bf->bit_array.resize(num_bytes);
    std::memcpy(bf->bit_array.data(), buf + sizeof(uint64_t) * 2, num_bytes);

    return bf;
}

void lumafly_destroy(LumaflyBloom* bf) {
    if (bf) {
        delete bf;
    }
}

} // extern "C"
