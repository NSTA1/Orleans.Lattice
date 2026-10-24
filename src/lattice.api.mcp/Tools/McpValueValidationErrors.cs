using System.Text.Json;
using Grpc.Core;
using ModelContextProtocol.Protocol;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>Projects payload-free validation diagnostics at the common MCP invocation seam.</summary>
internal static class McpValueValidationErrors
{
    public static CallToolResult? TryReport(Exception fault, IServiceProvider services, string toolName)
    {
        string code;
        string tree;
        string key;
        string reason;
        if (fault is ILatticeValueValidationFault validation)
        {
            code = validation.Code;
            tree = validation.TreeId;
            key = validation.Key;
            reason = validation.Reason;
        }
        else if (fault is RpcException { StatusCode: StatusCode.InvalidArgument } rpc
            && rpc.Trailers.GetValue("lattice-validation-code") is { } remoteCode)
        {
            code = remoteCode;
            tree = rpc.Trailers.GetValue("lattice-validation-tree") ?? string.Empty;
            key = rpc.Trailers.GetValue("lattice-validation-key") ?? string.Empty;
            reason = rpc.Trailers.GetValue("lattice-validation-reason") ?? "The value failed schema validation.";
        }
        else
        {
            return null;
        }

        code = McpToolClientErrors.SanitizeForEcho(code);
        tree = McpToolClientErrors.SanitizeForEcho(tree);
        key = McpToolClientErrors.SanitizeForEcho(key);
        reason = McpToolClientErrors.SanitizeForEcho(reason);
        var result = CredentialStampingTool.ReportClientError(
            services, toolName,
            $"The value was rejected by the tree's schema policy. {reason} Correct the value or update the policy before retrying.",
            McpToolClientErrorReason.RejectedContent);
        result.StructuredContent = JsonSerializer.SerializeToElement(new Dictionary<string, string>
        {
            ["code"] = code,
            ["treeId"] = tree,
            ["key"] = key,
            ["reason"] = reason,
        });
        return result;
    }
}
