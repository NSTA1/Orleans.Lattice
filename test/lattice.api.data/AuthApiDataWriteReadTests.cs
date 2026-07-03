using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// End-to-end coverage for the write-capable data-API facade routed through the
/// gated <see cref="ILattice"/> surface with a live access gate. Proves that the
/// caller's ambient identity scopes every mutation and read: an authorized write
/// is durable, an unauthorized or anonymous write is denied fail-closed, an
/// atomic batch aborts wholesale when a single leg is denied (no partial state),
/// a cross-tree batch aborts on a denied leg, a point read of a denied key
/// reports absent, and a bounded range read prunes to the authorized subset.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiDataWriteReadTests
{
    private const string Writer = "data-writer";
    private const string Reader = "data-reader";

    private AuthApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static IDisposable As(string subject) => AuthApiDataClusterFixture.AsSubject(subject);

    private LatticeAuthorizationRule AllowTree(string subject, string treeId, LatticeOperation ops) =>
        new($"{subject}-{treeId}-tree", LatticeSubjectSelector.User(subject), LatticeScope.Tree(treeId), ops, LatticeEffect.Allow);

    private LatticeAuthorizationRule AllowPrefix(string subject, string treeId, string prefix, LatticeOperation ops) =>
        new($"{subject}-{treeId}-{prefix}", LatticeSubjectSelector.User(subject), LatticeScope.Prefix(treeId, prefix), ops, LatticeEffect.Allow);

    private const LatticeOperation WriteOps =
        LatticeOperation.Write | LatticeOperation.Delete | LatticeOperation.AtomicWrite;

    private const LatticeOperation ReadOps =
        LatticeOperation.Read | LatticeOperation.RangeRead;

    [Test]
    public async Task authorized_set_persists_and_is_durable()
    {
        const string tree = "auth-set-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "k1", new byte[] { 42 });
        }

        var durable = await _fixture.ReadRawAsync(tree, "k1");
        Assert.That(durable, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public async Task authorized_get_returns_the_value()
    {
        const string tree = "auth-get-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        DataReadResult result;
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "k1", new byte[] { 7 });
            result = await _fixture.Api.GetAsync(tree, "k1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.True);
            Assert.That(result.Value, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public async Task unauthorized_set_is_denied()
    {
        const string tree = "auth-set-denied";
        await _fixture.RegisterTreeAsync(tree);

        using (As(Writer))
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Api.SetAsync(tree, "k1", new byte[] { 1 }));
        }

        // Nothing was persisted by the denied write.
        var durable = await _fixture.ReadRawAsync(tree, "k1");
        Assert.That(durable, Is.Null);
    }

    [Test]
    public async Task anonymous_set_is_denied_fail_closed()
    {
        const string tree = "auth-set-anon";
        await _fixture.RegisterTreeAsync(tree);

        // No ambient subject: the caller is anonymous and default-denied.
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await _fixture.Api.SetAsync(tree, "k1", new byte[] { 1 }));

        var durable = await _fixture.ReadRawAsync(tree, "k1");
        Assert.That(durable, Is.Null);
    }

    [Test]
    public async Task anonymous_get_reports_absent_fail_closed()
    {
        const string tree = "auth-get-anon";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "k1", new byte[] { 9 });
        }

        // Anonymous caller: the key exists but is hidden, so it reads back absent.
        var result = await _fixture.Api.GetAsync(tree, "k1");
        Assert.That(result.Found, Is.False);
    }

    [Test]
    public async Task authorized_delete_removes_the_value()
    {
        const string tree = "auth-delete-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        bool removed;
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "k1", new byte[] { 1 });
            removed = await _fixture.Api.DeleteAsync(tree, "k1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(_fixture.ReadRawAsync(tree, "k1").Result, Is.Null);
        });
    }

    [Test]
    public async Task unauthorized_delete_is_denied()
    {
        const string tree = "auth-delete-denied";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, LatticeOperation.Write));
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "k1", new byte[] { 1 });

            // Write is granted but Delete is not.
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Api.DeleteAsync(tree, "k1"));
        }

        Assert.That(_fixture.ReadRawAsync(tree, "k1").Result, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task atomic_batch_aborts_wholesale_when_one_leg_is_denied()
    {
        const string tree = "auth-atomic-denied";
        await _fixture.RegisterTreeAsync(tree);

        // Writer may write only under the "a/" prefix; the batch also touches
        // "b/1", which is denied, so the whole batch must abort.
        await _fixture.GrantAsync(AllowPrefix(Writer, tree, "a/", WriteOps));

        var batch = new DataAtomicBatch
        {
            Upserts =
            [
                new DataEntry { Key = "a/1", Value = new byte[] { 1 } },
                new DataEntry { Key = "b/1", Value = new byte[] { 2 } },
            ],
        };

        using (As(Writer))
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Api.SetManyAtomicAsync(tree, batch, "op-atomic-1"));
        }

        // No partial state: neither the allowed nor the denied key was applied.
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.ReadRawAsync(tree, "a/1").Result, Is.Null);
            Assert.That(_fixture.ReadRawAsync(tree, "b/1").Result, Is.Null);
        });
    }

    [Test]
    public async Task authorized_atomic_batch_commits_all_legs()
    {
        const string tree = "auth-atomic-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        var batch = new DataAtomicBatch
        {
            Upserts =
            [
                new DataEntry { Key = "a/1", Value = new byte[] { 1 } },
                new DataEntry { Key = "a/2", Value = new byte[] { 2 } },
            ],
        };

        using (As(Writer))
        {
            await _fixture.Api.SetManyAtomicAsync(tree, batch, "op-atomic-ok-1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(_fixture.ReadRawAsync(tree, "a/1").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.ReadRawAsync(tree, "a/2").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task cross_tree_atomic_aborts_when_one_leg_is_denied()
    {
        const string treeA = "auth-xt-a";
        const string treeB = "auth-xt-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);

        // Writer may write treeA but has no grant on treeB.
        await _fixture.GrantAsync(AllowTree(Writer, treeA, WriteOps));

        var batches = new List<DataTreeBatch>
        {
            new() { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
            new() { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
        };

        using (As(Writer))
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Api.SetManyAtomicCrossTreeAsync(batches, "op-xt-1"));
        }

        Assert.Multiple(() =>
        {
            Assert.That(_fixture.ReadRawAsync(treeA, "k").Result, Is.Null);
            Assert.That(_fixture.ReadRawAsync(treeB, "k").Result, Is.Null);
        });
    }

    [Test]
    public async Task authorized_cross_tree_atomic_commits_every_tree()
    {
        const string treeA = "auth-xt-ok-a";
        const string treeB = "auth-xt-ok-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);
        await _fixture.GrantAsync(
            AllowTree(Writer, treeA, WriteOps),
            AllowTree(Writer, treeB, WriteOps));

        var batches = new List<DataTreeBatch>
        {
            new() { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
            new() { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
        };

        CrossTreeAtomicWriteOutcome outcome;
        using (As(Writer))
        {
            outcome = await _fixture.Api.SetManyAtomicCrossTreeAsync(batches, "op-xt-ok-1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(_fixture.ReadRawAsync(treeA, "k").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.ReadRawAsync(treeB, "k").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task point_read_of_a_denied_key_reports_absent()
    {
        const string tree = "auth-read-scope";
        await _fixture.RegisterTreeAsync(tree);

        // Seed two keys under the admin, then grant the reader only "x/".
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "x/1", new byte[] { 1 });
            await _fixture.Api.SetAsync(tree, "y/1", new byte[] { 2 });
        }

        await _fixture.GrantAsync(AllowPrefix(Reader, tree, "x/", ReadOps));

        DataReadResult permitted;
        DataReadResult denied;
        using (As(Reader))
        {
            permitted = await _fixture.Api.GetAsync(tree, "x/1");
            denied = await _fixture.Api.GetAsync(tree, "y/1");
        }

        Assert.Multiple(() =>
        {
            Assert.That(permitted.Found, Is.True);
            Assert.That(permitted.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(denied.Found, Is.False, "a key outside the reader's grant reads back absent");
        });
    }

    [Test]
    public async Task bounded_range_read_prunes_to_the_authorized_subset()
    {
        const string tree = "auth-range-scope";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "x/1", new byte[] { 1 });
            await _fixture.Api.SetAsync(tree, "x/2", new byte[] { 2 });
            await _fixture.Api.SetAsync(tree, "y/1", new byte[] { 3 });
        }

        await _fixture.GrantAsync(AllowPrefix(Reader, tree, "x/", ReadOps));

        DataRangePage page;
        using (As(Reader))
        {
            page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 100 });
        }

        Assert.That(page.Entries.Select(e => e.Key), Is.EquivalentTo(new[] { "x/1", "x/2" }));
    }

    [Test]
    public async Task bounded_range_read_is_empty_for_an_anonymous_caller()
    {
        const string tree = "auth-range-anon";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        using (As(Writer))
        {
            await _fixture.Api.SetAsync(tree, "x/1", new byte[] { 1 });
        }

        // Anonymous caller: the range prunes to the empty authorized subset.
        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 100 });
        Assert.That(page.Entries, Is.Empty);
    }
}
