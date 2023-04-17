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
package ee.starrocks.sql.parser;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.PolicyType;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.ast.WithRowAccessPolicy;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.StarRocksParser;
import ee.starrocks.sql.ast.AlterPolicyStmt;
import ee.starrocks.sql.ast.CreatePolicyStmt;
import ee.starrocks.sql.ast.DropPolicyStmt;
import ee.starrocks.sql.ast.ShowCreatePolicyStmt;
import ee.starrocks.sql.ast.ShowPolicyApplyStmt;
import ee.starrocks.sql.ast.ShowPolicyStmt;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.util.stream.Collectors.toList;

public class AstBuilderEE extends AstBuilder {

    // ---------Alter Policy clause---------

    @Override
    public ParseNode visitApplyMaskingPolicyClause(StarRocksParser.ApplyMaskingPolicyClauseContext context) {
        String columName = ((Identifier) visit(context.identifier())).getValue();
        if (context.SET() != null) {
            List<String> usingColumns = new ArrayList<>();
            if (context.identifierList() != null) {
                final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
                usingColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
            }

            QualifiedName qualifiedName = getQualifiedName(context.policyName);
            PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

            WithColumnMaskingPolicy withColumnMaskingPolicy =
                    new WithColumnMaskingPolicy(policyName, usingColumns, createPos(context));

            return new ApplyMaskingPolicyClause(columName, withColumnMaskingPolicy, createPos(context));
        } else {
            return new RevokeMaskingPolicyClause(columName, createPos(context));
        }
    }

    @Override
    public ParseNode visitApplyRowAccessPolicyClause(StarRocksParser.ApplyRowAccessPolicyClauseContext context) {
        if (context.ADD() != null) {
            List<String> onColumns = new ArrayList<>();

            if (context.identifierList() != null) {
                final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
                onColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
            }

            QualifiedName qualifiedName = getQualifiedName(context.policyName);
            PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

            WithRowAccessPolicy withRowAccessPolicy = new WithRowAccessPolicy(policyName, onColumns, createPos(context));

            return new ApplyRowAccessPolicyClause(withRowAccessPolicy, createPos(context));
        } else {
            if (context.ALL() != null) {
                QualifiedName qualifiedName = getQualifiedName(context.policyName);
                PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
                return new RevokeRowAccessPolicyClause(policyName, createPos(context));
            } else {
                return new RevokeRowAccessPolicyClause(createPos(context));
            }
        }
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    @Override
    public ParseNode visitCreateMaskingPolicyStatement(StarRocksParser.CreateMaskingPolicyStatementContext context) {
        List<String> argNames = new ArrayList<>();
        List<TypeDef> argTypes = new ArrayList<>();
        for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
            argNames.add(((Identifier) visit(arg.identifier())).getValue());
            argTypes.add(new TypeDef(getType(arg.type())));
        }

        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        String comment = context.comment() == null ? "" : ((StringLiteral) visit(context.comment())).getStringValue();

        return new CreatePolicyStmt(context.OR() != null, context.IF() != null,
                PolicyType.COLUMN_MASKING, policyName, argNames, argTypes, new TypeDef(getType(context.type())),
                (Expr) visit(context.expression()), comment, createPos(context));
    }

    @Override
    public ParseNode visitDropMaskingPolicyStatement(StarRocksParser.DropMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new DropPolicyStmt(PolicyType.COLUMN_MASKING, policyName, context.IF() != null, context.FORCE() != null,
                createPos(context));
    }

    @Override
    public ParseNode visitAlterMaskingPolicyStatement(StarRocksParser.AlterMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        if (context.BODY() != null) {
            return new AlterPolicyStmt(PolicyType.COLUMN_MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetBody((Expr) visit(context.expression())), createPos(context));
        } else if (context.COMMENT() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new AlterPolicyStmt(PolicyType.COLUMN_MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetComment(stringLiteral.getValue()), createPos(context));
        } else {
            String newPolicyName = ((Identifier) visit(context.newPolicyName)).getValue();
            return new AlterPolicyStmt(PolicyType.COLUMN_MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicyRename(newPolicyName), createPos(context));
        }
    }

    @Override
    public ParseNode visitShowMaskingPolicyStatement(StarRocksParser.ShowMaskingPolicyStatementContext context) {
        String database = null;
        String catalog = null;
        // catalog.db
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            List<String> parts = qualifiedName.getParts();
            if (parts.size() == 2) {
                catalog = qualifiedName.getParts().get(0);
                database = qualifiedName.getParts().get(1);
            } else if (parts.size() == 1) {
                database = qualifiedName.getParts().get(0);
            }
        }

        return new ShowPolicyStmt(catalog, database, PolicyType.COLUMN_MASKING, createPos(context));
    }

    @Override
    public ParseNode visitShowCreateMaskingPolicyStatement(StarRocksParser.ShowCreateMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new ShowCreatePolicyStmt(PolicyType.COLUMN_MASKING, policyName, createPos(context));
    }

    @Override
    public ParseNode visitCreateRowAccessPolicyStatement(StarRocksParser.CreateRowAccessPolicyStatementContext context) {
        List<String> argNames = new ArrayList<>();
        List<TypeDef> argTypes = new ArrayList<>();
        for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
            argNames.add(((Identifier) visit(arg.identifier())).getValue());
            argTypes.add(new TypeDef(getType(arg.type())));
        }

        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        String comment = context.comment() == null ? "" : ((StringLiteral) visit(context.comment())).getStringValue();

        return new CreatePolicyStmt(context.OR() != null, context.IF() != null,
                PolicyType.ROW_ACCESS, policyName, argNames, argTypes, new TypeDef(Type.BOOLEAN),
                (Expr) visit(context.expression()), comment, createPos(context));
    }

    @Override
    public ParseNode visitAlterRowAccessPolicyStatement(StarRocksParser.AlterRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        if (context.SET() != null) {
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetBody((Expr) visit(context.expression())), createPos(context));
        } else if (context.COMMENT() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetComment(stringLiteral.getValue()), createPos(context));
        } else {
            String newPolicyName = ((Identifier) visit(context.newPolicyName)).getValue();
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicyRename(newPolicyName), createPos(context));
        }
    }

    @Override
    public ParseNode visitDropRowAccessPolicyStatement(StarRocksParser.DropRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new DropPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null, context.FORCE() != null,
                createPos(context));
    }

    @Override
    public ParseNode visitShowRowAccessPolicyStatement(StarRocksParser.ShowRowAccessPolicyStatementContext context) {
        String database = null;
        String catalog = null;
        // catalog.db
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            List<String> parts = qualifiedName.getParts();
            if (parts.size() == 2) {
                catalog = qualifiedName.getParts().get(0);
                database = qualifiedName.getParts().get(1);
            } else if (parts.size() == 1) {
                database = qualifiedName.getParts().get(0);
            }
        }

        return new ShowPolicyStmt(catalog, database, PolicyType.ROW_ACCESS, createPos(context));
    }

    @Override
    public ParseNode visitShowCreateRowAccessPolicyStatement(StarRocksParser.ShowCreateRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new ShowCreatePolicyStmt(PolicyType.ROW_ACCESS, policyName, createPos(context));
    }

    @Override
    public ParseNode visitShowPolicyReferences(StarRocksParser.ShowPolicyReferencesContext context) {
        return new ShowPolicyApplyStmt(createPos(context));
    }


    @Override
    public ParseNode visitWithMaskingPolicy(StarRocksParser.WithMaskingPolicyContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        List<String> usingColumns = new ArrayList<>();
        if (context.identifierList() != null) {
            final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            usingColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
        }

        return new WithColumnMaskingPolicy(policyName, usingColumns, createPos(context));
    }

    @Override
    public ParseNode visitWithRowAccessPolicy(StarRocksParser.WithRowAccessPolicyContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        List<String> onColumns = new ArrayList<>();
        if (context.identifierList() != null) {
            final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            onColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
        }

        return new WithRowAccessPolicy(policyName, onColumns, createPos(context));
    }

    private PolicyName qualifiedNameToPolicyName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.policy_name
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 3) {
            return new PolicyName(parts.get(0), parts.get(1), parts.get(2), qualifiedName.getPos());
        } else if (parts.size() == 2) {
            return new PolicyName(null, qualifiedName.getParts().get(0), qualifiedName.getParts().get(1),
                    qualifiedName.getPos());
        } else if (parts.size() == 1) {
            return new PolicyName(null, null, qualifiedName.getParts().get(0), qualifiedName.getPos());
        } else {
            throw new ParsingException(PARSER_ERROR_MSG.invalidTableFormat(qualifiedName.toString()));
        }
    }
}
