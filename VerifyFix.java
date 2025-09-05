import java.util.Arrays;
import java.util.List;

// 模拟相关的类
class AnalysisException extends Exception {
    public AnalysisException(String message) {
        super(message);
    }
}

class BackendClause {
    protected List<String> hostPorts;
    
    public BackendClause(List<String> hostPorts) {
        this.hostPorts = hostPorts;
    }
    
    public void analyze(Object analyzer) throws AnalysisException {
        // 模拟analyze方法
    }
    
    public String toSql() {
        return "BASE BACKEND SQL";
    }
}

class AddBackendClause extends BackendClause {
    protected boolean isFree;
    protected String destCluster;
    
    public AddBackendClause(List<String> hostPorts) {
        super(hostPorts);
        this.isFree = true;
        this.destCluster = "";
    }
    
    public AddBackendClause(List<String> hostPorts, boolean isFree) {
        super(hostPorts);
        this.isFree = isFree;
        this.destCluster = "";
    }
    
    public AddBackendClause(List<String> hostPorts, String destCluster) {
        super(hostPorts);
        this.isFree = false;
        this.destCluster = destCluster;
    }
    
    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        if (isFree) {
            sb.append("FREE ");
        }
        sb.append("BACKEND ");

        if (destCluster != null && !destCluster.isEmpty()) {
            sb.append(" to ").append(destCluster).append(" ");
        }

        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}

class DropBackendClause extends BackendClause {
    public DropBackendClause(List<String> hostPorts) {
        super(hostPorts);
    }
    
    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP BACKEND ");
        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}

public class VerifyFix {
    public static void main(String[] args) {
        System.out.println("=== 验证 AddBackendClause 修复 ===");
        
        // 测试 case 4: normal add (isFree = true, destCluster = "")
        AddBackendClause clause4 = new AddBackendClause(Arrays.asList("192.168.1.1:12345"));
        String result4 = clause4.toSql();
        String expected4 = "ADD FREE BACKEND \"192.168.1.1:12345\"";
        
        System.out.println("Test case 4 (normal add):");
        System.out.println("Expected: " + expected4);
        System.out.println("Actual:   " + result4);
        System.out.println("Match:    " + expected4.equals(result4));
        System.out.println();
        
        // 测试 case 5: normal remove
        DropBackendClause clause5 = new DropBackendClause(Arrays.asList("192.168.1.2:12345"));
        String result5 = clause5.toSql();
        String expected5 = "DROP BACKEND \"192.168.1.2:12345\"";
        
        System.out.println("Test case 5 (normal remove):");
        System.out.println("Expected: " + expected5);
        System.out.println("Actual:   " + result5);
        System.out.println("Match:    " + expected5.equals(result5));
        System.out.println();
        
        // 测试带集群名的情况
        AddBackendClause clauseWithCluster = new AddBackendClause(Arrays.asList("192.168.1.3:12345"), "test_cluster");
        String resultWithCluster = clauseWithCluster.toSql();
        String expectedWithCluster = "ADD BACKEND to test_cluster \"192.168.1.3:12345\"";
        
        System.out.println("Test with cluster name:");
        System.out.println("Expected: " + expectedWithCluster);
        System.out.println("Actual:   " + resultWithCluster);
        System.out.println("Match:    " + expectedWithCluster.equals(resultWithCluster));
        System.out.println();
        
        // 测试非free的情况
        AddBackendClause clauseNotFree = new AddBackendClause(Arrays.asList("192.168.1.4:12345"), false);
        String resultNotFree = clauseNotFree.toSql();
        String expectedNotFree = "ADD BACKEND \"192.168.1.4:12345\"";
        
        System.out.println("Test not free:");
        System.out.println("Expected: " + expectedNotFree);
        System.out.println("Actual:   " + resultNotFree);
        System.out.println("Match:    " + expectedNotFree.equals(resultNotFree));
    }
}