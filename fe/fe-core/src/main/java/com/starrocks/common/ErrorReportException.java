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

package com.starrocks.common;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;

public class ErrorReportException extends RuntimeException {
    private final ErrorCode errorCode;

    private ErrorReportException(ErrorCode errorCode, String errorMsg) {
        super(errorMsg);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static ErrorReportException report(ErrorCode errorCode, Object... objs) {
        String errMsg = errorCode.formatErrorMsg(objs);
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            ctx.getState().setError(errMsg);
            ctx.getState().setErrorCode(errorCode);

            if (errorCode.getSqlState()[0] == 'X' && errorCode.getSqlState()[1] == 'X') {
                ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            }

            if (errorCode.getSqlState()[1] == '5' && errorCode.getSqlState()[2] == '3' &&
                    errorCode.getSqlState()[3] == '4') {
                ctx.getState().setErrType(QueryState.ErrType.EXEC_TIME_OUT);
            }
        }
        return new ErrorReportException(errorCode, errMsg);
    }
}
