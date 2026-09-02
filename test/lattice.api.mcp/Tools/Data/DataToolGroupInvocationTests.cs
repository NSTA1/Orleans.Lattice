using System.Text;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests that drive every <see cref="DataToolGroup"/> tool's own invocation
/// delegate through <see cref="McpToolInvocation"/>: the static
/// <c>*ToolAsync</c> bodies that resolve <see cref="ILatticeDataApi"/> from the
/// request service provider, decode the base64 argument shapes, and forward to
/// <c>DataToolCore</c>. The sibling <see cref="DataToolGroupTests"/> covers only
/// the advertised metadata, which never reaches these bodies.
/// </summary>
/// <remarks>
/// The group advertises thirty-four tools, and the argument names it binds are
/// part of its contract with an agent: a renamed or dropped parameter breaks
/// every caller while leaving a metadata-only test green. The table below drives
/// each tool by its advertised name with a realistic argument map, so a binding
/// that no longer matches fails here. All deterministic - an in-memory facade
/// fake, no cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class DataToolGroupInvocationTests
{
    private const string Tree = "orders";
    private const string Replica = "replica-a";

    private static string B64(string text) => Convert.ToBase64String(Encoding.UTF8.GetBytes(text));

    private FakeDataApi _api = null!;

    [SetUp]
    public void SetUp() => _api = new FakeDataApi();

    private ServiceProvider Services()
        => new ServiceCollection().AddSingleton<ILatticeDataApi>(_api).BuildServiceProvider();

    private static McpServerTool Tool(string name)
        => new DataToolGroup(enableWrites: true).Tools.Single(t => t.ProtocolTool.Name == name);

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(
            Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    /// <summary>
    /// One realistic argument map per advertised tool. Kept beside the group so a
    /// newly added tool that is never invoked is caught by the completeness test
    /// below rather than silently going untested.
    /// </summary>
    private static IEnumerable<TestCaseData> ToolInvocations()
    {
        (string Name, object? Value)[] point = [("treeId", Tree), ("key", "k")];

        yield return Case("lattice_data_get", point);
        yield return Case("lattice_data_read_range",
            ("treeId", Tree), ("startInclusive", "a"), ("endExclusive", "z"), ("pageSize", 10), ("continuationToken", null));
        yield return Case("lattice_data_set", ("treeId", Tree), ("key", "k"), ("value", B64("v")));
        yield return Case("lattice_data_delete", point);
        yield return Case("lattice_data_delete_range",
            ("treeId", Tree), ("startInclusive", "a"), ("endExclusive", "z"));
        yield return Case("lattice_data_set_many",
            ("treeId", Tree),
            ("upserts", new[] { new DataEntryDto { Key = "k", Value = Encoding.UTF8.GetBytes("v") } }));
        yield return Case("lattice_data_set_many_atomic",
            ("treeId", Tree),
            ("upserts", new[] { new DataEntryDto { Key = "k", Value = Encoding.UTF8.GetBytes("v") } }),
            ("deleteKeys", new[] { "gone" }),
            ("operationId", "op-1"));
        yield return Case("lattice_data_set_many_atomic_cross_tree",
            ("batches", new[]
            {
                new DataTreeBatchDto
                {
                    TreeId = Tree,
                    Upserts = [new DataEntryDto { Key = "k", Value = Encoding.UTF8.GetBytes("v") }],
                },
            }),
            ("operationId", "op-2"));

        yield return Case("lattice_data_pncounter",
            ("treeId", Tree), ("key", "c"), ("operation", CrdtCounterOp.Increment), ("replicaId", Replica), ("amount", 3L));
        yield return Case("lattice_data_pncounter_get", ("treeId", Tree), ("key", "c"));
        yield return Case("lattice_data_gcounter",
            ("treeId", Tree), ("key", "g"), ("replicaId", Replica), ("amount", 2L));
        yield return Case("lattice_data_gcounter_get", ("treeId", Tree), ("key", "g"));
        yield return Case("lattice_data_orset",
            ("treeId", Tree), ("key", "s"), ("operation", CrdtSetOp.Add), ("element", B64("e")), ("replicaId", Replica));
        yield return Case("lattice_data_orset_get", ("treeId", Tree), ("key", "s"));
        yield return Case("lattice_data_orflag",
            ("treeId", Tree), ("key", "f"), ("operation", CrdtFlagOp.Enable), ("replicaId", Replica));
        yield return Case("lattice_data_orflag_get", ("treeId", Tree), ("key", "f"));
        yield return Case("lattice_data_rwflag",
            ("treeId", Tree), ("key", "rf"), ("operation", CrdtFlagOp.Disable), ("replicaId", Replica));
        yield return Case("lattice_data_rwflag_get", ("treeId", Tree), ("key", "rf"));
        yield return Case("lattice_data_rwset",
            ("treeId", Tree), ("key", "rs"), ("operation", CrdtRwSetOp.Add), ("element", B64("e")), ("replicaId", Replica));
        yield return Case("lattice_data_rwset_get", ("treeId", Tree), ("key", "rs"));
        yield return Case("lattice_data_version_vector_tick",
            ("treeId", Tree), ("key", "vv"), ("replicaId", Replica));
        yield return Case("lattice_data_version_vector_get", ("treeId", Tree), ("key", "vv"));
        yield return Case("lattice_data_mvregister_set",
            ("treeId", Tree), ("key", "mv"), ("replicaId", Replica), ("value", B64("v")));
        yield return Case("lattice_data_mvregister_get", ("treeId", Tree), ("key", "mv"));
        yield return Case("lattice_data_maxregister_set", ("treeId", Tree), ("key", "mx"), ("value", B64("v")));
        yield return Case("lattice_data_maxregister_get", ("treeId", Tree), ("key", "mx"));
        yield return Case("lattice_data_minregister_set", ("treeId", Tree), ("key", "mn"), ("value", B64("v")));
        yield return Case("lattice_data_minregister_get", ("treeId", Tree), ("key", "mn"));
        yield return Case("lattice_data_sequence",
            ("treeId", Tree), ("key", "q"), ("operation", CrdtSequenceOp.InsertAt), ("index", 0),
            ("replicaId", Replica), ("value", B64("v")));
        yield return Case("lattice_data_sequence_get", ("treeId", Tree), ("key", "q"));
        yield return Case("lattice_data_ormap",
            ("treeId", Tree), ("key", "m"), ("operation", CrdtMapOp.Set), ("field", "colour"),
            ("replicaId", Replica), ("value", B64("v")));
        yield return Case("lattice_data_ormap_get", ("treeId", Tree), ("key", "m"));
        yield return Case("lattice_data_gset", ("treeId", Tree), ("key", "gs"), ("element", B64("e")));
        yield return Case("lattice_data_gset_get", ("treeId", Tree), ("key", "gs"));

        static TestCaseData Case(string name, params (string Name, object? Value)[] args)
            => new TestCaseData(name, args).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(ToolInvocations))]
    public async Task Tool_delegate_binds_its_advertised_arguments_and_reaches_the_facade(
        string toolName,
        (string Name, object? Value)[] arguments)
    {
        await using var services = Services();

        var result = await McpToolInvocation.CallAsync(
            Tool(toolName), services, McpToolInvocation.Args(arguments));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True,
                $"The '{toolName}' delegate must bind its advertised argument names and reach the facade.");
            Assert.That(result.StructuredContent, Is.Not.Null,
                $"The '{toolName}' tool advertises structured content, so its delegate must return some.");
        });
    }

    [Test]
    public void Every_advertised_tool_is_exercised_by_the_invocation_table()
    {
        var advertised = new DataToolGroup(enableWrites: true)
            .Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);
        var exercised = ToolInvocations()
            .Select(c => (string)c.Arguments[0]!).ToHashSet(StringComparer.Ordinal);

        Assert.That(exercised, Is.EquivalentTo(advertised),
            "Every advertised data tool must have its invocation delegate driven, so a newly added tool "
            + "cannot ship with an untested binding.");
    }

    // ---- the argument shapes the delegates own ------------------------------

    [Test]
    public async Task Set_tool_delegate_decodes_the_base64_value_before_it_reaches_the_facade()
    {
        await CallAsync<DataSetToolResult>(
            "lattice_data_set", ("treeId", Tree), ("key", "k"), ("value", B64("hello")));

        var read = await CallAsync<DataGetToolResult>("lattice_data_get", ("treeId", Tree), ("key", "k"));

        Assert.That(read.Found, Is.True);
        Assert.That(Encoding.UTF8.GetString(read.Value!), Is.EqualTo("hello"),
            "The delegate must base64-decode the value argument, so the facade stores the raw bytes.");
    }

    [Test]
    public void Set_tool_delegate_rejects_a_non_base64_value_as_a_caller_error()
    {
        Assert.That(
            async () => await CallAsync<DataSetToolResult>(
                "lattice_data_set", ("treeId", Tree), ("key", "k"), ("value", "not base64!")),
            Throws.Exception.Message.Contains("base64"),
            "A non-base64 value is a caller error and must surface as a clean, self-contained message.");
    }

    [Test]
    public async Task Sequence_tool_delegate_treats_an_omitted_value_as_no_value()
    {
        await CallAsync<CrdtWriteToolResult>(
            "lattice_data_sequence",
            ("treeId", Tree), ("key", "q"), ("operation", CrdtSequenceOp.InsertAt),
            ("index", 0), ("replicaId", Replica), ("value", B64("first")));

        var removed = await CallAsync<CrdtWriteToolResult>(
            "lattice_data_sequence",
            ("treeId", Tree), ("key", "q"), ("operation", CrdtSequenceOp.RemoveAt),
            ("index", 0), ("replicaId", Replica));

        Assert.That(removed.Committed, Is.True,
            "removeAt ignores the value argument, so an omitted value must decode to null rather than fault.");
    }

    [Test]
    public async Task Map_tool_delegate_treats_an_omitted_value_as_no_value()
    {
        await CallAsync<CrdtWriteToolResult>(
            "lattice_data_ormap",
            ("treeId", Tree), ("key", "m"), ("operation", CrdtMapOp.Set),
            ("field", "colour"), ("replicaId", Replica), ("value", B64("red")));

        var removed = await CallAsync<CrdtWriteToolResult>(
            "lattice_data_ormap",
            ("treeId", Tree), ("key", "m"), ("operation", CrdtMapOp.Remove),
            ("field", "colour"), ("replicaId", Replica));

        Assert.That(removed.Committed, Is.True,
            "remove ignores the value argument, so an omitted value must decode to null rather than fault.");
    }

    [Test]
    public async Task Set_many_tool_delegate_forwards_every_upsert()
    {
        var result = await CallAsync<DataSetManyToolResult>(
            "lattice_data_set_many",
            ("treeId", Tree),
            ("upserts", new[]
            {
                new DataEntryDto { Key = "a", Value = Encoding.UTF8.GetBytes("1") },
                new DataEntryDto { Key = "b", Value = Encoding.UTF8.GetBytes("2") },
            }));

        Assert.That(result.Count, Is.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(_api.Contains(Tree, "a"), Is.True);
            Assert.That(_api.Contains(Tree, "b"), Is.True);
        });
    }

    [Test]
    public async Task Set_many_atomic_tool_delegate_forwards_upserts_deletes_and_the_operation_id()
    {
        await CallAsync<DataSetToolResult>(
            "lattice_data_set", ("treeId", Tree), ("key", "gone"), ("value", B64("x")));

        var result = await CallAsync<DataAtomicBatchToolResult>(
            "lattice_data_set_many_atomic",
            ("treeId", Tree),
            ("upserts", new[] { new DataEntryDto { Key = "kept", Value = Encoding.UTF8.GetBytes("v") } }),
            ("deleteKeys", new[] { "gone" }),
            ("operationId", "op-1"));

        Assert.That(result.OperationId, Is.EqualTo("op-1"));
        Assert.Multiple(() =>
        {
            Assert.That(_api.Contains(Tree, "kept"), Is.True, "The upsert leg must reach the facade.");
            Assert.That(_api.Contains(Tree, "gone"), Is.False, "The delete leg must reach the facade.");
        });
    }

    [Test]
    public async Task Cross_tree_tool_delegate_forwards_every_tree_slice()
    {
        var result = await CallAsync<DataCrossTreeBatchToolResult>(
            "lattice_data_set_many_atomic_cross_tree",
            ("batches", new[]
            {
                new DataTreeBatchDto
                {
                    TreeId = "orders",
                    Upserts = [new DataEntryDto { Key = "o", Value = Encoding.UTF8.GetBytes("1") }],
                },
                new DataTreeBatchDto
                {
                    TreeId = "inventory",
                    Upserts = [new DataEntryDto { Key = "i", Value = Encoding.UTF8.GetBytes("2") }],
                },
            }),
            ("operationId", "op-2"));

        Assert.That(result.Committed, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(_api.Contains("orders", "o"), Is.True);
            Assert.That(_api.Contains("inventory", "i"), Is.True);
        });
    }

    [Test]
    public async Task Read_range_tool_delegate_forwards_the_range_bounds()
    {
        foreach (var key in new[] { "a", "m", "z" })
        {
            await CallAsync<DataSetToolResult>(
                "lattice_data_set", ("treeId", Tree), ("key", key), ("value", B64(key)));
        }

        var page = await CallAsync<DataRangePageToolResult>(
            "lattice_data_read_range",
            ("treeId", Tree), ("startInclusive", "a"), ("endExclusive", "n"), ("pageSize", 10));

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "m" }),
            "The delegate must forward both range bounds, so the exclusive upper bound is honoured.");
    }

    [Test]
    public async Task Delete_range_tool_delegate_forwards_the_range_bounds()
    {
        foreach (var key in new[] { "a", "m", "z" })
        {
            await CallAsync<DataSetToolResult>(
                "lattice_data_set", ("treeId", Tree), ("key", key), ("value", B64(key)));
        }

        var result = await CallAsync<DataRangeDeleteToolResult>(
            "lattice_data_delete_range",
            ("treeId", Tree), ("startInclusive", "a"), ("endExclusive", "n"));

        Assert.That(result.DeletedCount, Is.EqualTo(2));
        Assert.That(_api.Contains(Tree, "z"), Is.True, "A key outside the range must survive.");
    }

    [Test]
    public async Task Counter_tool_delegate_forwards_the_operation_and_amount()
    {
        await CallAsync<CrdtWriteToolResult>(
            "lattice_data_pncounter",
            ("treeId", Tree), ("key", "c"), ("operation", CrdtCounterOp.Increment),
            ("replicaId", Replica), ("amount", 5L));
        await CallAsync<CrdtWriteToolResult>(
            "lattice_data_pncounter",
            ("treeId", Tree), ("key", "c"), ("operation", CrdtCounterOp.Decrement),
            ("replicaId", Replica), ("amount", 2L));

        var value = await CallAsync<CrdtCounterToolResult>(
            "lattice_data_pncounter_get", ("treeId", Tree), ("key", "c"));

        Assert.That(value.Value, Is.EqualTo(3),
            "The delegate must forward both the operation discriminator and the magnitude.");
    }

    [Test]
    public void Delegate_surfaces_the_facades_fail_closed_denial_on_a_write()
    {
        _api.Denied.Add((Tree, "locked"));

        Assert.That(
            async () => await CallAsync<DataSetToolResult>(
                "lattice_data_set", ("treeId", Tree), ("key", "locked"), ("value", B64("v"))),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path: the facade's denial must surface unchanged.");
    }

    [Test]
    public void Resolving_the_facade_without_a_request_service_provider_is_a_clean_failure()
    {
        var tool = Tool("lattice_data_get");

        Assert.That(
            async () => await McpToolInvocation.CallAsync(
                tool,
                new ServiceCollection().BuildServiceProvider(),
                McpToolInvocation.Args(("treeId", Tree), ("key", "k"))),
            Throws.InstanceOf<InvalidOperationException>(),
            "With no ILatticeDataApi registered the delegate must fail cleanly rather than null-reference.");
    }
}
