using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// The data-plane capability probe (#5367) against a live access gate: it answers
/// exactly as the write path would, per key and per tree, with no side effects.
/// </summary>
public sealed partial class AuthApiDataWriteReadTests
{
    [Test]
    public async Task probe_reports_write_and_delete_under_a_whole_tree_grant()
    {
        const string tree = "probe-whole";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        LatticeDataCapabilities whole;
        LatticeDataCapabilities point;
        using (As(Writer))
        {
            whole = await _fixture.Api.ProbeCapabilitiesAsync(tree);
            point = await _fixture.Api.ProbeCapabilitiesAsync(tree, "k1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(whole, Is.EqualTo(new LatticeDataCapabilities { TreeId = tree, CanWrite = true, CanDelete = true }));
            Assert.That(point, Is.EqualTo(new LatticeDataCapabilities { TreeId = tree, Key = "k1", CanWrite = true, CanDelete = true }));
        });
    }

    [Test]
    public async Task probe_under_a_prefix_grant_allows_the_tree_and_keys_inside_the_prefix_only()
    {
        const string tree = "probe-prefix";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowPrefix(Writer, tree, "orders/", WriteOps | ReadOps));

        LatticeDataCapabilities whole;
        LatticeDataCapabilities inside;
        LatticeDataCapabilities outside;
        using (As(Writer))
        {
            whole = await _fixture.Api.ProbeCapabilitiesAsync(tree);
            inside = await _fixture.Api.ProbeCapabilitiesAsync(tree, "orders/1");
            outside = await _fixture.Api.ProbeCapabilitiesAsync(tree, "invoices/1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(whole.CanWrite, Is.True, "a prefix grant writes some key of the tree");
            Assert.That(inside.CanWrite, Is.True);
            Assert.That(inside.CanDelete, Is.True);
            Assert.That(outside.CanWrite, Is.False);
            Assert.That(outside.CanDelete, Is.False);
        });
    }

    [Test]
    public async Task probe_matches_enforcement_for_a_read_only_caller()
    {
        const string tree = "probe-read-only";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Reader, tree, ReadOps));

        LatticeDataCapabilities whole;
        LatticeDataCapabilities point;
        using (As(Reader))
        {
            whole = await _fixture.Api.ProbeCapabilitiesAsync(tree);
            point = await _fixture.Api.ProbeCapabilitiesAsync(tree, "k1");
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Api.SetAsync(tree, "k1", [1]));
        }

        Assert.Multiple(() =>
        {
            Assert.That(whole.CanWrite || whole.CanDelete, Is.False);
            Assert.That(point.CanWrite || point.CanDelete, Is.False);
        });
    }

    [Test]
    public async Task probe_reports_write_without_delete_when_only_write_is_granted()
    {
        const string tree = "probe-write-only";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, LatticeOperation.Write | ReadOps));

        LatticeDataCapabilities point;
        using (As(Writer))
        {
            point = await _fixture.Api.ProbeCapabilitiesAsync(tree, "k1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(point.CanWrite, Is.True);
            Assert.That(point.CanDelete, Is.False);
        });
    }

    [Test]
    public async Task probe_is_all_false_for_an_anonymous_caller_and_writes_nothing()
    {
        const string tree = "probe-anon";
        await _fixture.RegisterTreeAsync(tree);

        var whole = await _fixture.Api.ProbeCapabilitiesAsync(tree);
        var point = await _fixture.Api.ProbeCapabilitiesAsync(tree, "k1");
        var durable = await _fixture.ReadRawAsync(tree, "k1");

        Assert.Multiple(() =>
        {
            Assert.That(whole.CanWrite || whole.CanDelete, Is.False);
            Assert.That(point.CanWrite || point.CanDelete, Is.False);
            Assert.That(durable, Is.Null);
        });
    }

    [Test]
    public void probe_rejects_a_missing_tree_id()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentException>(async () => await _fixture.Api.ProbeCapabilitiesAsync(string.Empty));
            Assert.ThrowsAsync<ArgumentNullException>(async () => await _fixture.Api.ProbeCapabilitiesAsync(null!));
        });
    }
}
