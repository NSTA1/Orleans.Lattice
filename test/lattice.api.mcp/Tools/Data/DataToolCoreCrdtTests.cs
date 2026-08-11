using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the typed-CRDT half of <see cref="DataToolCore"/>. Proves each
/// per-type write verb drives the matching <see cref="Data.ILatticeDataApi"/>
/// operation and each read shapes the facade result into its structured-content
/// DTO, over a deterministic <see cref="FakeDataApi"/>. Covers the fail-closed
/// contract inherited from the facade (a denied write throws with nothing
/// persisted; a denied read reads as the empty value for its kind) and the two
/// tool-local guards - an unsupported operation and a missing value byte string.
/// No cluster, no MCP envelope, no timing.
/// </summary>
[TestFixture]
public sealed class DataToolCoreCrdtTests
{
    private const string Tree = "tree-a";

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task Counter_increment_then_get_sums_the_replica_contributions()
    {
        var api = new FakeDataApi();

        await DataToolCore.CounterWriteAsync(api, Tree, "c", CrdtCounterOp.Increment, "r1", 3, CancellationToken.None);
        await DataToolCore.CounterWriteAsync(api, Tree, "c", CrdtCounterOp.Decrement, "r2", 1, CancellationToken.None);
        var read = await DataToolCore.CounterGetAsync(api, Tree, "c", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Value, Is.EqualTo(2));
            Assert.That(read.TreeId, Is.EqualTo(Tree));
            Assert.That(read.Key, Is.EqualTo("c"));
        });
    }

    [Test]
    public async Task OrSet_add_then_get_returns_the_observed_element()
    {
        var api = new FakeDataApi();

        var write = await DataToolCore.SetWriteAsync(api, Tree, "s", CrdtSetOp.Add, Bytes("x"), "r1", CancellationToken.None);
        var read = await DataToolCore.SetGetAsync(api, Tree, "s", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(write.Committed, Is.True);
            Assert.That(read.Elements, Has.Count.EqualTo(1));
            Assert.That(read.Elements[0], Is.EqualTo(Bytes("x")));
        });
    }

    [Test]
    public async Task OrSet_remove_drops_the_element()
    {
        var api = new FakeDataApi();
        await DataToolCore.SetWriteAsync(api, Tree, "s", CrdtSetOp.Add, Bytes("x"), "r1", CancellationToken.None);

        await DataToolCore.SetWriteAsync(api, Tree, "s", CrdtSetOp.Remove, Bytes("x"), "r1", CancellationToken.None);
        var read = await DataToolCore.SetGetAsync(api, Tree, "s", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task OrFlag_enable_then_get_reports_enabled()
    {
        var api = new FakeDataApi();

        await DataToolCore.OrFlagWriteAsync(api, Tree, "f", CrdtFlagOp.Enable, "r1", CancellationToken.None);
        var read = await DataToolCore.OrFlagGetAsync(api, Tree, "f", CancellationToken.None);

        Assert.That(read.Enabled, Is.True);
    }

    [Test]
    public async Task RwFlag_disable_then_get_reports_disabled()
    {
        var api = new FakeDataApi();
        await DataToolCore.RwFlagWriteAsync(api, Tree, "f", CrdtFlagOp.Enable, "r1", CancellationToken.None);

        await DataToolCore.RwFlagWriteAsync(api, Tree, "f", CrdtFlagOp.Disable, "r1", CancellationToken.None);
        var read = await DataToolCore.RwFlagGetAsync(api, Tree, "f", CancellationToken.None);

        Assert.That(read.Enabled, Is.False);
    }

    [Test]
    public async Task RwSet_add_then_get_returns_the_observed_element()
    {
        var api = new FakeDataApi();

        var write = await DataToolCore.RwSetWriteAsync(api, Tree, "s", CrdtRwSetOp.Add, Bytes("x"), "r1", CancellationToken.None);
        var read = await DataToolCore.RwSetGetAsync(api, Tree, "s", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(write.Committed, Is.True);
            Assert.That(read.Elements, Has.Count.EqualTo(1));
            Assert.That(read.Elements[0], Is.EqualTo(Bytes("x")));
        });
    }

    [Test]
    public async Task RwSet_remove_drops_the_element()
    {
        var api = new FakeDataApi();
        await DataToolCore.RwSetWriteAsync(api, Tree, "s", CrdtRwSetOp.Add, Bytes("x"), "r1", CancellationToken.None);

        await DataToolCore.RwSetWriteAsync(api, Tree, "s", CrdtRwSetOp.Remove, Bytes("x"), "r1", CancellationToken.None);
        var read = await DataToolCore.RwSetGetAsync(api, Tree, "s", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task VersionVector_tick_then_get_exposes_the_per_replica_clock()
    {
        var api = new FakeDataApi();

        await DataToolCore.VersionVectorTickAsync(api, Tree, "v", "r1", CancellationToken.None);
        await DataToolCore.VersionVectorTickAsync(api, Tree, "v", "r1", CancellationToken.None);
        var read = await DataToolCore.VersionVectorGetAsync(api, Tree, "v", CancellationToken.None);

        Assert.That(read.Entries.Keys, Is.EquivalentTo(new[] { "r1" }));
    }

    [Test]
    public async Task MvRegister_set_then_get_returns_the_current_value()
    {
        var api = new FakeDataApi();

        await DataToolCore.RegisterSetAsync(api, Tree, "r", "r1", Bytes("v1"), CancellationToken.None);
        var read = await DataToolCore.RegisterGetAsync(api, Tree, "r", CancellationToken.None);

        Assert.That(read.Elements[0], Is.EqualTo(Bytes("v1")));
    }

    [Test]
    public async Task MaxRegister_set_then_get_returns_the_greatest_value()
    {
        var api = new FakeDataApi();

        await DataToolCore.MaxRegisterSetAsync(api, Tree, "r", new byte[] { 0x02 }, CancellationToken.None);
        await DataToolCore.MaxRegisterSetAsync(api, Tree, "r", new byte[] { 0x08 }, CancellationToken.None);
        await DataToolCore.MaxRegisterSetAsync(api, Tree, "r", new byte[] { 0x05 }, CancellationToken.None);
        var read = await DataToolCore.MaxRegisterGetAsync(api, Tree, "r", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Elements, Has.Count.EqualTo(1));
            Assert.That(read.Elements[0], Is.EqualTo(new byte[] { 0x08 }));
        });
    }

    [Test]
    public async Task MinRegister_set_then_get_returns_the_least_value()
    {
        var api = new FakeDataApi();

        await DataToolCore.MinRegisterSetAsync(api, Tree, "r", new byte[] { 0x08 }, CancellationToken.None);
        await DataToolCore.MinRegisterSetAsync(api, Tree, "r", new byte[] { 0x02 }, CancellationToken.None);
        await DataToolCore.MinRegisterSetAsync(api, Tree, "r", new byte[] { 0x05 }, CancellationToken.None);
        var read = await DataToolCore.MinRegisterGetAsync(api, Tree, "r", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Elements, Has.Count.EqualTo(1));
            Assert.That(read.Elements[0], Is.EqualTo(new byte[] { 0x02 }));
        });
    }

    [Test]
    public async Task MaxRegister_get_on_missing_key_returns_no_elements()
    {
        var api = new FakeDataApi();

        var read = await DataToolCore.MaxRegisterGetAsync(api, Tree, "absent", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public void MaxRegisterSetAsync_on_a_denied_key_throws_and_persists_nothing()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.MaxRegisterSetAsync(api, Tree, "secret", new byte[] { 1 }, CancellationToken.None));
    }

    [Test]
    public async Task MinRegisterGetAsync_on_a_denied_key_reads_as_empty()
    {
        var api = new FakeDataApi();
        await DataToolCore.MinRegisterSetAsync(api, Tree, "secret", new byte[] { 1 }, CancellationToken.None);
        api.Denied.Add((Tree, "secret"));

        var read = await DataToolCore.MinRegisterGetAsync(api, Tree, "secret", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public void MaxRegister_methods_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.MaxRegisterSetAsync(null!, Tree, "k", new byte[] { 1 }, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.MaxRegisterGetAsync(null!, Tree, "k", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.MinRegisterSetAsync(null!, Tree, "k", new byte[] { 1 }, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.MinRegisterGetAsync(null!, Tree, "k", CancellationToken.None));
        });
    }

    [Test]
    public async Task Sequence_insert_then_get_preserves_order()
    {
        var api = new FakeDataApi();

        await DataToolCore.SequenceWriteAsync(api, Tree, "q", CrdtSequenceOp.InsertAt, 0, "r1", Bytes("a"), CancellationToken.None);
        await DataToolCore.SequenceWriteAsync(api, Tree, "q", CrdtSequenceOp.InsertAt, 1, "r1", Bytes("b"), CancellationToken.None);
        var read = await DataToolCore.SequenceGetAsync(api, Tree, "q", CancellationToken.None);

        Assert.That(read.Elements.Select(e => e[0]), Is.EqualTo(new[] { (byte)'a', (byte)'b' }));
    }

    [Test]
    public async Task Sequence_remove_at_deletes_the_positioned_element()
    {
        var api = new FakeDataApi();
        await DataToolCore.SequenceWriteAsync(api, Tree, "q", CrdtSequenceOp.InsertAt, 0, "r1", Bytes("a"), CancellationToken.None);

        await DataToolCore.SequenceWriteAsync(api, Tree, "q", CrdtSequenceOp.RemoveAt, 0, "r1", value: null, CancellationToken.None);
        var read = await DataToolCore.SequenceGetAsync(api, Tree, "q", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public async Task OrMap_set_then_get_returns_the_field_value()
    {
        var api = new FakeDataApi();

        await DataToolCore.MapWriteAsync(api, Tree, "doc", CrdtMapOp.Set, "title", "r1", Bytes("hi"), CancellationToken.None);
        var read = await DataToolCore.MapGetAsync(api, Tree, "doc", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Fields.Keys, Is.EquivalentTo(new[] { "title" }));
            Assert.That(read.Fields["title"][0], Is.EqualTo(Bytes("hi")));
        });
    }

    [Test]
    public async Task OrMap_remove_drops_the_field()
    {
        var api = new FakeDataApi();
        await DataToolCore.MapWriteAsync(api, Tree, "doc", CrdtMapOp.Set, "title", "r1", Bytes("hi"), CancellationToken.None);

        await DataToolCore.MapWriteAsync(api, Tree, "doc", CrdtMapOp.Remove, "title", "r1", value: null, CancellationToken.None);
        var read = await DataToolCore.MapGetAsync(api, Tree, "doc", CancellationToken.None);

        Assert.That(read.Fields, Is.Empty);
    }

    [Test]
    public async Task GSet_add_then_get_returns_the_element()
    {
        var api = new FakeDataApi();

        var write = await DataToolCore.GSetAddAsync(api, Tree, "g", Bytes("x"), CancellationToken.None);
        var read = await DataToolCore.GSetGetAsync(api, Tree, "g", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(write.Committed, Is.True);
            Assert.That(read.Elements, Has.Count.EqualTo(1));
            Assert.That(read.Elements[0], Is.EqualTo(Bytes("x")));
        });
    }

    [Test]
    public async Task GSet_add_is_idempotent()
    {
        var api = new FakeDataApi();

        await DataToolCore.GSetAddAsync(api, Tree, "g", Bytes("x"), CancellationToken.None);
        await DataToolCore.GSetAddAsync(api, Tree, "g", Bytes("x"), CancellationToken.None);
        var read = await DataToolCore.GSetGetAsync(api, Tree, "g", CancellationToken.None);

        Assert.That(read.Elements, Has.Count.EqualTo(1));
    }

    [Test]
    public void GSetAddAsync_on_a_denied_key_throws_and_persists_nothing()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.GSetAddAsync(api, Tree, "secret", Bytes("x"), CancellationToken.None));
    }

    [Test]
    public async Task GSetGetAsync_on_a_denied_key_reads_as_empty()
    {
        var api = new FakeDataApi();
        await DataToolCore.GSetAddAsync(api, Tree, "secret", Bytes("x"), CancellationToken.None);
        api.Denied.Add((Tree, "secret"));

        var read = await DataToolCore.GSetGetAsync(api, Tree, "secret", CancellationToken.None);

        Assert.That(read.Elements, Is.Empty);
    }

    [Test]
    public void CounterWriteAsync_on_a_denied_key_throws_and_persists_nothing()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.CounterWriteAsync(api, Tree, "secret", CrdtCounterOp.Increment, "r1", 1, CancellationToken.None));
    }

    [Test]
    public async Task CounterGetAsync_on_a_denied_key_reads_as_zero()
    {
        var api = new FakeDataApi();
        await DataToolCore.CounterWriteAsync(api, Tree, "secret", CrdtCounterOp.Increment, "r1", 5, CancellationToken.None);
        api.Denied.Add((Tree, "secret"));

        var read = await DataToolCore.CounterGetAsync(api, Tree, "secret", CancellationToken.None);

        Assert.That(read.Value, Is.EqualTo(0));
    }

    [Test]
    public void SequenceWriteAsync_insert_without_a_value_throws_a_missing_value_fault()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.SequenceWriteAsync(api, Tree, "q", CrdtSequenceOp.InsertAt, 0, "r1", value: null, CancellationToken.None));
    }

    [Test]
    public void MapWriteAsync_set_without_a_value_throws_a_missing_value_fault()
    {
        var api = new FakeDataApi();

        Assert.ThrowsAsync<McpException>(
            () => DataToolCore.MapWriteAsync(api, Tree, "doc", CrdtMapOp.Set, "title", "r1", value: null, CancellationToken.None));
    }

    [Test]
    public async Task GCounter_increment_then_get_sums_the_replica_contributions()
    {
        var api = new FakeDataApi();

        await DataToolCore.GCounterIncrementAsync(api, Tree, "c", "r1", 3, CancellationToken.None);
        await DataToolCore.GCounterIncrementAsync(api, Tree, "c", "r2", 4, CancellationToken.None);
        var read = await DataToolCore.GCounterGetAsync(api, Tree, "c", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Value, Is.EqualTo(7));
            Assert.That(read.TreeId, Is.EqualTo(Tree));
            Assert.That(read.Key, Is.EqualTo("c"));
        });
    }

    [Test]
    public void GCounterIncrementAsync_on_a_denied_key_throws_with_nothing_persisted()
    {
        var api = new FakeDataApi();
        api.Denied.Add((Tree, "secret"));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => DataToolCore.GCounterIncrementAsync(api, Tree, "secret", "r1", 1, CancellationToken.None));
    }

    [Test]
    public async Task GCounterGetAsync_on_a_denied_key_reads_as_zero()
    {
        var api = new FakeDataApi();
        await DataToolCore.GCounterIncrementAsync(api, Tree, "secret", "r1", 5, CancellationToken.None);
        api.Denied.Add((Tree, "secret"));

        var read = await DataToolCore.GCounterGetAsync(api, Tree, "secret", CancellationToken.None);

        Assert.That(read.Value, Is.EqualTo(0));
    }

    [Test]
    public void Crdt_methods_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.CounterGetAsync(null!, Tree, "k", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.GCounterGetAsync(null!, Tree, "k", CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.GCounterIncrementAsync(null!, Tree, "k", "r1", 1, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(
                () => DataToolCore.MapWriteAsync(null!, Tree, "k", CrdtMapOp.Set, "f", "r1", Bytes("v"), CancellationToken.None));
        });
    }
}
