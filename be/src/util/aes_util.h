// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/aes_util.h

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

#include <stdint.h>

namespace starrocks {

enum AesMode { AES_128_ECB, AES_192_ECB, AES_256_ECB, AES_128_CBC, AES_192_CBC, AES_256_CBC };

enum AesState { AES_SUCCESS = 0, AES_BAD_DATA = -1 };

class AesUtil {
public:
    static int encrypt(AesMode mode, const unsigned char* source, uint32_t source_length, const unsigned char* key,
                       uint32_t key_length, const unsigned char* iv, bool padding, unsigned char* encrypt);

    static int decrypt(AesMode mode, const unsigned char* encrypt, uint32_t encrypt_length, const unsigned char* key,
                       uint32_t key_length, const unsigned char* iv, bool padding, unsigned char* decrypt_content);
};

} // namespace starrocks
