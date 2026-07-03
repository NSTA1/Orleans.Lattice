using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Durable-cursor enforcement (OC-1) and non-recursion (OC-2) coverage for the
/// authorization control point.
/// <para>
/// <b>OC-1.</b> A durable cursor grain pages the tree on behalf of the caller.
/// These tests prove the original caller's identity propagates on the Orleans
/// <see cref="Orleans.Runtime.RequestContext"/> from the client, through the
/// cursor grain, into the tree read - so a cursor observes only the keys policy
/// admits for that caller. The live key / entry cursors ride the public filtered
/// scan surface; the snapshot cursors read snapshot leaf grains directly and have
/// the key-filter re-applied at page emit. An anonymous (non-caller) drain is
/// fail-closed.
/// </para>
/// <para>
/// <b>OC-2.</b> With the membership directory active and the enforcing gate live,
/// a normal user's range scan resolves the caller subject and applies the filter
/// without recursing back into the gate (subject resolution runs under a
/// system-origin scope) and without aborting the enumeration.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeCursorEnforcementIntegrationTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private async Task SeedAsync(string tree)
    {
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("a:2", Bytes("2"));
            await seed.SetAsync("a:3", Bytes("3"));
            await seed.SetAsync("b:1", Bytes("4"));
            await seed.SetAsync("b:2", Bytes("5"));
        }
    }

    private async Task GrantRangeReadPrefixAsync(string tree, string subject, string prefix)
    {
        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            $"cur-{subject}", LatticeSubjectSelector.User(subject), LatticeScope.Prefix(tree, prefix),
            LatticeOperation.RangeRead, LatticeEffect.Allow));
        await _fixture.RebuildPolicyAsync();
    }

    private async Task<List<string>> DrainKeyCursorAsync(string tree, bool snapshot)
    {
        var lattice = _fixture.Lattice(tree);
        var cursorId = snapshot
            ? await lattice.OpenSnapshotKeyCursorAsync()
            : await lattice.OpenKeyCursorAsync();
        var keys = new List<string>();
        try
        {
            while (true)
            {
                var page = await lattice.NextKeysAsync(cursorId, pageSize: 2);
                keys.AddRange(page.Keys);
                if (!page.HasMore)
                {
                    break;
                }
            }
        }
        finally
        {
            await lattice.CloseCursorAsync(cursorId);
        }

        return keys;
    }

    private async Task<List<string>> DrainEntryCursorAsync(string tree, bool snapshot)
    {
        var lattice = _fixture.Lattice(tree);
        var cursorId = snapshot
            ? await lattice.OpenSnapshotEntryCursorAsync()
            : await lattice.OpenEntryCursorAsync();
        var keys = new List<string>();
        try
        {
            while (true)
            {
                var page = await lattice.NextEntriesAsync(cursorId, pageSize: 2);
                foreach (var entry in page.Entries)
                {
                    keys.Add(entry.Key);
                }

                if (!page.HasMore)
                {
                    break;
                }
            }
        }
        finally
        {
            await lattice.CloseCursorAsync(cursorId);
        }

        return keys;
    }

    [Test]
    public async Task Durable_key_cursor_observes_only_the_authorized_prefix()
    {
        const string tree = "cur-key-prefix";
        await SeedAsync(tree);
        await GrantRangeReadPrefixAsync(tree, "kcur", "a:");

        using (AuthClusterFixture.AsSubject("kcur"))
        {
            var keys = await DrainKeyCursorAsync(tree, snapshot: false);
            Assert.That(keys, Is.EquivalentTo(new[] { "a:1", "a:2", "a:3" }),
                "the caller identity propagates into the durable key cursor's paging, so only the granted prefix is observed");
        }
    }

    [Test]
    public async Task Durable_entry_cursor_observes_only_the_authorized_prefix()
    {
        const string tree = "cur-entry-prefix";
        await SeedAsync(tree);
        await GrantRangeReadPrefixAsync(tree, "ecur", "a:");

        using (AuthClusterFixture.AsSubject("ecur"))
        {
            var keys = await DrainEntryCursorAsync(tree, snapshot: false);
            Assert.That(keys, Is.EquivalentTo(new[] { "a:1", "a:2", "a:3" }),
                "the caller identity propagates into the durable entry cursor's paging");
        }
    }

    [Test]
    public async Task Durable_key_cursor_with_no_rule_yields_nothing()
    {
        const string tree = "cur-key-deny";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("cur-nobody"))
        {
            var keys = await DrainKeyCursorAsync(tree, snapshot: false);
            Assert.That(keys, Is.Empty, "fail-closed: an unauthorized caller's durable cursor observes nothing");
        }
    }

    [Test]
    public async Task Durable_key_cursor_opened_anonymously_yields_nothing()
    {
        const string tree = "cur-key-anon";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        // No AsSubject scope: the caller resolves to LatticeSubject.Anonymous and a
        // fail-closed default-deny policy admits nothing.
        var keys = await DrainKeyCursorAsync(tree, snapshot: false);
        Assert.That(keys, Is.Empty, "an anonymous durable cursor is fail-closed, never allowed through");
    }

    [Test]
    public async Task Snapshot_key_cursor_observes_only_the_authorized_prefix()
    {
        const string tree = "cur-snap-key";
        await SeedAsync(tree);
        await GrantRangeReadPrefixAsync(tree, "skcur", "a:");

        using (AuthClusterFixture.AsSubject("skcur"))
        {
            var keys = await DrainKeyCursorAsync(tree, snapshot: true);
            Assert.That(keys, Is.EquivalentTo(new[] { "a:1", "a:2", "a:3" }),
                "the snapshot key cursor re-applies the caller's key-filter at page emit");
        }
    }

    [Test]
    public async Task Snapshot_entry_cursor_observes_only_the_authorized_prefix()
    {
        const string tree = "cur-snap-entry";
        await SeedAsync(tree);
        await GrantRangeReadPrefixAsync(tree, "secur", "a:");

        using (AuthClusterFixture.AsSubject("secur"))
        {
            var keys = await DrainEntryCursorAsync(tree, snapshot: true);
            Assert.That(keys, Is.EquivalentTo(new[] { "a:1", "a:2", "a:3" }),
                "the snapshot entry cursor re-applies the caller's key-filter at page emit");
        }
    }

    [Test]
    public async Task Snapshot_key_cursor_with_no_rule_yields_nothing()
    {
        const string tree = "cur-snap-deny";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("snap-nobody"))
        {
            var keys = await DrainKeyCursorAsync(tree, snapshot: true);
            Assert.That(keys, Is.Empty, "fail-closed: an unauthorized caller's snapshot cursor observes nothing");
        }
    }

    [Test]
    public async Task Range_scan_with_membership_active_resolves_and_filters_without_recursing()
    {
        // OC-2: with the membership directory active and the enforcing gate live,
        // a normal user's range scan must resolve the subject and apply the filter
        // without recursing back into the gate or aborting the enumeration.
        const string tree = "oc2-range";
        await SeedAsync(tree);
        await GrantRangeReadPrefixAsync(tree, "oc2-user", "a:");

        var observed = new List<string>();
        using (AuthClusterFixture.AsSubject("oc2-user", "some-group"))
        {
            Assert.That(async () =>
            {
                await foreach (var key in _fixture.Lattice(tree).KeysAsync())
                {
                    observed.Add(key);
                }
            }, Throws.Nothing, "the enforced range scan must not throw EnumerationAbortedException or recurse");
        }

        Assert.That(observed, Is.EquivalentTo(new[] { "a:1", "a:2", "a:3" }),
            "the scan resolves the subject and applies the prefix filter to completion");
    }
}
