// C ABI wrapper for StarRocks format SDK.
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema;
struct ArrowArray;

int starrocks_format_c_init(char* err_buf, int err_len);

int starrocks_format_c_shutdown(char* err_buf, int err_len);

void* starrocks_format_reader_create(int64_t tablet_id, const char* tablet_root_path, int64_t version,
                                     struct ArrowSchema* required_schema, struct ArrowSchema* output_schema,
                                     const char* const* option_keys, const char* const* option_values,
                                     size_t option_len, char* err_buf, int err_len);

int starrocks_format_reader_open(void* reader, char* err_buf, int err_len);

int starrocks_format_reader_get_next(void* reader, struct ArrowArray* out_array, char* err_buf, int err_len);

void starrocks_format_reader_close(void* reader);

void starrocks_format_reader_release(void* reader);

#ifdef __cplusplus
}
#endif
