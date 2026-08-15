#ifndef LUMAFLY_BLOOM_H
#define LUMAFLY_BLOOM_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque struct pointer for Go cgo interface
typedef struct LumaflyBloom LumaflyBloom;

// Lumafly Bloom Filter C-API functions
LumaflyBloom* lumafly_create(size_t expected_entries, double fp_rate);
void lumafly_add(LumaflyBloom* bf, const char* key, size_t len);
bool lumafly_contains(const LumaflyBloom* bf, const char* key, size_t len);
size_t lumafly_get_serialized_size(const LumaflyBloom* bf);
size_t lumafly_serialize(const LumaflyBloom* bf, char* buf, size_t buf_len);
LumaflyBloom* lumafly_deserialize(const char* buf, size_t len);
void lumafly_destroy(LumaflyBloom* bf);

#ifdef __cplusplus
}
#endif

#endif // LUMAFLY_BLOOM_H
