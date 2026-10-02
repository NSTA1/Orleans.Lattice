using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Covers the tool-local unsupported-operation guard that every multi-verb CRDT
/// mapping in <see cref="DataToolCore"/> carries - the <c>_ =&gt; throw
/// UnknownOperation(...)</c> arm of each <c>operation switch</c>.
/// <para>
/// Each of these arms is unreachable through the MCP tool surface, because the
/// binder only ever materialises a declared enum member, so no amount of testing
/// through <c>DataToolGroup</c> reaches them. They are nonetheless the contract
/// that keeps the mapping total: a member added to one of the seven
/// <c>Crdt*Op</c> enums without a matching facade verb must surface as a clean
/// <see cref="McpException"/> rather than silently selecting a neighbouring verb
/// or returning a committed result for a write that never happened. Reaching
/// them needs a direct call with an undeclared enum value, which is what this
/// fixture does.
/// </para>
/// <para>
/// All seven are asserted, one per multi-verb mapping, so a mapping that grows a
/// third verb cannot quietly lose its guard. Each assertion also reads the key
/// back and finds the empty value for its kind, because the switch arm throws
/// while <i>composing</i> the facade task rather than after awaiting it, so a
/// rejected operation must leave nothing behind.
/// </para>
/// </summary>
[TestFixture]
public sealed class DataToolCoreCrdtUnknownOperationTests
{
    private const string Tree = "tree-a";

    /// <summary>A value outside every declared member of every <c>Crdt*Op</c> enum.</summary>
    private const int Undeclared = 99;

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task CounterWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.CounterWriteAsync(
                api, Tree, "c", (CrdtCounterOp)Undeclared, "r1", 1, CancellationToken.None));

        var read = await DataToolCore.CounterGetAsync(api, Tree, "c", CancellationToken.None);
        Assert.That(read.Value, Is.Zero, "the guard throws before any facade verb is selected");
    }

    [Test]
    public async Task SetWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.SetWriteAsync(
                api, Tree, "s", (CrdtSetOp)Undeclared, Bytes("x"), "r1", CancellationToken.None));

        var read = await DataToolCore.SetGetAsync(api, Tree, "s", CancellationToken.None);
        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task OrFlagWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.OrFlagWriteAsync(
                api, Tree, "f", (CrdtFlagOp)Undeclared, "r1", CancellationToken.None));

        var read = await DataToolCore.OrFlagGetAsync(api, Tree, "f", CancellationToken.None);
        Assert.That(read.Enabled, Is.False);
    }

    [Test]
    public async Task RwFlagWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.RwFlagWriteAsync(
                api, Tree, "f", (CrdtFlagOp)Undeclared, "r1", CancellationToken.None));

        var read = await DataToolCore.RwFlagGetAsync(api, Tree, "f", CancellationToken.None);
        Assert.That(read.Enabled, Is.False);
    }

    [Test]
    public async Task RwSetWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.RwSetWriteAsync(
                api, Tree, "s", (CrdtRwSetOp)Undeclared, Bytes("x"), "r1", CancellationToken.None));

        var read = await DataToolCore.RwSetGetAsync(api, Tree, "s", CancellationToken.None);
        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task SequenceWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.SequenceWriteAsync(
                api, Tree, "q", (CrdtSequenceOp)Undeclared, 0, "r1", Bytes("v"), CancellationToken.None));

        var read = await DataToolCore.SequenceGetAsync(api, Tree, "q", CancellationToken.None);
        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task SequenceWriteAsync_checks_the_operation_before_the_missing_value_guard()
    {
        // An undeclared operation supplied with no value must name the operation,
        // not the value: the switch arm is reached first, so the two tool-local
        // guards cannot be mistaken for one another.
        var api = new FakeDataApi();

        var fault = Assert.ThrowsAsync<McpException>(
            () => DataToolCore.SequenceWriteAsync(
                api, Tree, "q", (CrdtSequenceOp)Undeclared, 0, "r1", value: null, CancellationToken.None));

        Assert.That(fault!.Message, Does.Contain("operation").And.Not.Contains("base64"));

        var read = await DataToolCore.SequenceGetAsync(api, Tree, "q", CancellationToken.None);
        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task MapWriteAsync_rejects_an_undeclared_operation()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.MapWriteAsync(
                api, Tree, "doc", (CrdtMapOp)Undeclared, "title", "r1", Bytes("v"), CancellationToken.None));

        var read = await DataToolCore.MapGetAsync(api, Tree, "doc", CancellationToken.None);
        Assert.That(read.Fields, Is.Empty);
    }

    [Test]
    public void MapWriteAsync_checks_the_operation_before_the_missing_value_guard()
    {
        var api = new FakeDataApi();

        var fault = Assert.ThrowsAsync<McpException>(
            () => DataToolCore.MapWriteAsync(
                api, Tree, "doc", (CrdtMapOp)Undeclared, "title", "r1", value: null, CancellationToken.None));

        Assert.That(fault!.Message, Does.Contain("operation").And.Not.Contains("base64"));
    }

    [Test]
    public void The_unsupported_operation_fault_names_the_offending_parameter()
    {
        // The guard is shared, so one assertion on its message suffices - but it
        // has to be made somewhere, or a message that stopped naming the parameter
        // would still pass every arm above.
        var api = new FakeDataApi();

        var fault = Assert.ThrowsAsync<McpException>(
            () => DataToolCore.CounterWriteAsync(
                api, Tree, "c", (CrdtCounterOp)Undeclared, "r1", 1, CancellationToken.None));

        Assert.That(fault!.Message, Does.Contain("'operation'"));
    }
}
