namespace Orleans.Lattice.Api.Mcp.Tests;

public sealed partial class CredentialStampingToolClientErrorTests
{
    [Test]
    public async Task A_remote_validation_refusal_is_a_structured_rejected_content_result()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw new Grpc.Core.RpcException(
            new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument, "must not echo raw remote details"),
            new Grpc.Core.Metadata
            {
                { "lattice-validation-code", "schema-policy" },
                { "lattice-validation-tree", "orders" },
                { "lattice-validation-key", "k" },
                { "lattice-validation-reason", "The value is not a well-formed JSON document." },
            }));
        var logs = new CapturingLoggerProvider();
        await using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "secret-value")));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(ErrorText(result), Does.Contain("JSON").And.Contain("Correct the value"));
            Assert.That(ErrorText(result), Does.Not.Contain("secret-value").And.Not.Contain("raw remote"));
            Assert.That(result.StructuredContent!.Value.GetProperty("code").GetString(), Is.EqualTo("schema-policy"));
            Assert.That(result.StructuredContent.Value.GetProperty("treeId").GetString(), Is.EqualTo("orders"));
            Assert.That(result.StructuredContent.Value.GetProperty("key").GetString(), Is.EqualTo("k"));
            Assert.That(counter.Reasons, Is.EqualTo(new[] { LatticeApiMcpMetrics.ReasonRejectedContent }));
        });
        AssertLoggedAtDebugOnly(logs, name, LatticeApiMcpMetrics.ReasonRejectedContent);
    }

    [Test]
    public async Task A_local_validation_refusal_is_a_safe_structured_error()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw new ValidationFault());
        await using var services = Services(new CapturingLoggerProvider());

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "secret-value")));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(result.StructuredContent!.Value.GetProperty("reason").GetString(), Is.EqualTo("JSON required?"));
            Assert.That(ErrorText(result), Does.Not.Contain("secret-value").And.Not.Contain("raw-fault-secret"));
        });
    }

    private sealed class ValidationFault : Exception, ILatticeValueValidationFault
    {
        public ValidationFault() : base("raw-fault-secret") { }
        public string Code => "schema-policy";
        public string TreeId => "orders";
        public string Key => "k";
        public string Reason => "JSON required\n";
    }
}
