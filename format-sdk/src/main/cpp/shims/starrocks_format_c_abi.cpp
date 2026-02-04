// C ABI wrapper for StarRocks format SDK.

#include "shims/starrocks_format_c_abi.h"

#include <arrow/c/abi.h>
#include <arrow/status.h>

#include <cstring>
#include <string>
#include <unordered_map>

#include "format/starrocks_format_reader.h"
#include "starrocks_format/starrocks_lib.h"

namespace {
void write_error(char* buf, int len, const std::string& msg) {
    if (buf == nullptr || len <= 0) {
        return;
    }
    const size_t max_len = static_cast<size_t>(len - 1);
    const size_t n = msg.size() < max_len ? msg.size() : max_len;
    if (n > 0) {
        std::memcpy(buf, msg.data(), n);
    }
    buf[n] = '\0';
}

std::unordered_map<std::string, std::string> build_options(const char* const* keys, const char* const* values,
                                                           size_t len) {
    std::unordered_map<std::string, std::string> out;
    out.reserve(len);
    if (keys == nullptr || values == nullptr) {
        return out;
    }
    for (size_t i = 0; i < len; ++i) {
        const char* key = keys[i];
        const char* value = values[i];
        if (key == nullptr) {
            continue;
        }
        out.emplace(key, value == nullptr ? "" : value);
    }
    return out;
}
} // namespace

extern "C" {

int starrocks_format_c_init(char* err_buf, int err_len) {
    try {
        starrocks::lake::starrocks_format_initialize();
        return 0;
    } catch (const std::exception& e) {
        write_error(err_buf, err_len, e.what());
        return 1;
    } catch (...) {
        write_error(err_buf, err_len, "unknown error");
        return 1;
    }
}

int starrocks_format_c_shutdown(char* err_buf, int err_len) {
    try {
        starrocks::lake::starrocks_format_shutdown();
        return 0;
    } catch (const std::exception& e) {
        write_error(err_buf, err_len, e.what());
        return 1;
    } catch (...) {
        write_error(err_buf, err_len, "unknown error");
        return 1;
    }
}

void* starrocks_format_reader_create(int64_t tablet_id, const char* tablet_root_path, int64_t version,
                                     ArrowSchema* required_schema, ArrowSchema* output_schema,
                                     const char* const* option_keys, const char* const* option_values,
                                     size_t option_len, char* err_buf, int err_len) {
    if (tablet_root_path == nullptr) {
        write_error(err_buf, err_len, "tablet_root_path is null");
        return nullptr;
    }
    if (required_schema == nullptr) {
        write_error(err_buf, err_len, "required_schema is null");
        return nullptr;
    }
    if (output_schema == nullptr) {
        write_error(err_buf, err_len, "output_schema is null");
        return nullptr;
    }
    if (option_len > 0 && (option_keys == nullptr || option_values == nullptr)) {
        write_error(err_buf, err_len, "option keys/values are null");
        return nullptr;
    }

    std::unordered_map<std::string, std::string> options = build_options(option_keys, option_values, option_len);

    try {
        auto result = starrocks::lake::format::StarRocksFormatReader::create(
                tablet_id, std::string(tablet_root_path), version, required_schema, output_schema,
                std::move(options));
        if (!result.ok()) {
            write_error(err_buf, err_len, result.status().message());
            return nullptr;
        }
        starrocks::lake::format::StarRocksFormatReader* reader = std::move(result).ValueUnsafe();
        return reinterpret_cast<void*>(reader);
    } catch (const std::exception& e) {
        write_error(err_buf, err_len, e.what());
        return nullptr;
    } catch (...) {
        write_error(err_buf, err_len, "unknown error");
        return nullptr;
    }
}

int starrocks_format_reader_open(void* reader, char* err_buf, int err_len) {
    if (reader == nullptr) {
        write_error(err_buf, err_len, "reader is null");
        return 1;
    }
    try {
        auto* handle = reinterpret_cast<starrocks::lake::format::StarRocksFormatReader*>(reader);
        arrow::Status st = handle->open();
        if (!st.ok()) {
            write_error(err_buf, err_len, st.message());
            return 1;
        }
        return 0;
    } catch (const std::exception& e) {
        write_error(err_buf, err_len, e.what());
        return 1;
    } catch (...) {
        write_error(err_buf, err_len, "unknown error");
        return 1;
    }
}

int starrocks_format_reader_get_next(void* reader, ArrowArray* out_array, char* err_buf, int err_len) {
    if (reader == nullptr) {
        write_error(err_buf, err_len, "reader is null");
        return 1;
    }
    if (out_array == nullptr) {
        write_error(err_buf, err_len, "out_array is null");
        return 1;
    }
    try {
        auto* handle = reinterpret_cast<starrocks::lake::format::StarRocksFormatReader*>(reader);
        arrow::Status st = handle->get_next(out_array);
        if (!st.ok()) {
            write_error(err_buf, err_len, st.message());
            return 1;
        }
        return 0;
    } catch (const std::exception& e) {
        write_error(err_buf, err_len, e.what());
        return 1;
    } catch (...) {
        write_error(err_buf, err_len, "unknown error");
        return 1;
    }
}

void starrocks_format_reader_close(void* reader) {
    if (reader == nullptr) {
        return;
    }
    auto* handle = reinterpret_cast<starrocks::lake::format::StarRocksFormatReader*>(reader);
    handle->close();
}

void starrocks_format_reader_release(void* reader) {
    if (reader == nullptr) {
        return;
    }
    auto* handle = reinterpret_cast<starrocks::lake::format::StarRocksFormatReader*>(reader);
    delete handle;
}

} // extern "C"
