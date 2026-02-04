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

#include "exprs/java_function_call_expr.h"

namespace starrocks {

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

JavaFunctionCallExpr::~JavaFunctionCallExpr() = default;

StatusOr<ColumnPtr> JavaFunctionCallExpr::evaluate_checked(ExprContext* /*context*/, Chunk* /*ptr*/) {
    return Status::NotSupported("Java UDF is not supported in macOS format-lib build");
}

Status JavaFunctionCallExpr::prepare(RuntimeState* /*state*/, ExprContext* /*context*/) {
    return Status::NotSupported("Java UDF is not supported in macOS format-lib build");
}

Status JavaFunctionCallExpr::open(RuntimeState* /*state*/, ExprContext* /*context*/,
                                  FunctionContext::FunctionStateScope /*scope*/) {
    return Status::NotSupported("Java UDF is not supported in macOS format-lib build");
}

void JavaFunctionCallExpr::close(RuntimeState* /*state*/, ExprContext* /*context*/,
                                 FunctionContext::FunctionStateScope /*scope*/) {}

bool JavaFunctionCallExpr::is_constant() const {
    // Be conservative: treat it as non-constant to avoid unexpected execution-time behavior.
    return false;
}

} // namespace starrocks

