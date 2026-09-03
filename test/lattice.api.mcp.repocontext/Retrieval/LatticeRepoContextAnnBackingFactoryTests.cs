using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Vector;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit coverage for <see cref="LatticeRepoContextAnnBackingFactory"/>, the
/// shipped binding between the approximate retrieval plane and real Lattice trees,
/// driven against a substituted index tree so the reclamation walk's own logic is
/// exercised rather than an in-memory double's.
/// <para>
/// The walk is the part worth pinning down. It is a <b>skip scan</b>: one bounded
/// key read names a whole space and the cursor then jumps past that space's
/// prefix, so its cost is one read per space rather than one per record. Every
/// step it takes is also a range delete, so a walk that miscomputed a prefix would
/// not merely read too much - it would delete the live index.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeRepoContextAnnBackingFactoryTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag LiveSpace = new("new-model", 8, VectorNormalization.UnitL2);
    private static readonly EmbeddingSpaceTag OldSpace = new("old-model", 8, VectorNormalization.UnitL2);
    private static readonly EmbeddingSpaceTag OlderSpace = new("older-model", 16, VectorNormalization.None);

    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// A substituted index tree holding keys in ascending ordinal order, exposing
    /// exactly the two operations the reclamation walk uses: the bounded key-only
    /// scan it advances its cursor with, and the range delete it retires a space
    /// with.
    /// </summary>
    private sealed class IndexTree
    {
        private readonly SortedDictionary<string, byte[]> _keys = new(StringComparer.Ordinal);

        public IndexTree()
        {
            Tree = Substitute.For<ILattice>();

            Tree.KeysAsync().ReturnsForAnyArgs(call => Scan(
                _keys, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

            Tree.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .ReturnsForAnyArgs(call =>
                {
                    var start = call.ArgAt<string>(0);
                    var end = call.ArgAt<string>(1);
                    var doomed = _keys.Keys
                        .Where(k => string.CompareOrdinal(k, start) >= 0 && string.CompareOrdinal(k, end) < 0)
                        .ToList();
                    foreach (var key in doomed)
                    {
                        _keys.Remove(key);
                    }

                    DeletedRanges.Add((start, end));
                    return Task.FromResult(doomed.Count);
                });

            GrainFactory = Substitute.For<IGrainFactory>();
            GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Tree);
        }

        public ILattice Tree { get; }

        public IGrainFactory GrainFactory { get; }

        /// <summary>Every range delete the walk issued, in order.</summary>
        public List<(string Start, string End)> DeletedRanges { get; } = [];

        public IReadOnlyList<string> Keys => [.. _keys.Keys];

        public void Put(params string[] keys)
        {
            foreach (var key in keys)
            {
                _keys[key] = [1];
            }
        }

        /// <summary>Seeds a handful of records under one repository/space prefix.</summary>
        public void PutSpace(string repoId, EmbeddingSpaceTag space, int count)
        {
            var prefix = RepoContextAnnIndexKeys.IndexPrefix(repoId, space);
            for (var i = 0; i < count; i++)
            {
                Put($"{prefix}chunk/{i:D4}");
            }

            Put($"{prefix}m");
        }

        private static async IAsyncEnumerable<string> Scan(
            SortedDictionary<string, byte[]> keys, string? startInclusive, string? endExclusive)
        {
            foreach (var key in keys.Keys)
            {
                if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
                {
                    continue;
                }

                if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
                {
                    break;
                }

                yield return key;
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }
    }

    private LatticeRepoContextAnnBackingFactory Factory(IndexTree tree)
        => new(tree.GrainFactory, _serializer);

    [Test]
    public void A_null_grain_factory_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => new LatticeRepoContextAnnBackingFactory(null!, _serializer));

    [Test]
    public void A_null_serializer_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => new LatticeRepoContextAnnBackingFactory(Substitute.For<IGrainFactory>(), null!));

    [Test]
    public void The_key_prefix_is_unique_per_repository_and_space()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeRepoContextAnnBackingFactory.KeyPrefix(RepoId, LiveSpace),
                Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix(RepoId, OldSpace)),
                "two spaces sharing a prefix would range-delete each other's generations");
            Assert.That(
                LatticeRepoContextAnnBackingFactory.KeyPrefix(RepoId, LiveSpace),
                Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("other", LiveSpace)));
            Assert.That(
                LatticeRepoContextAnnBackingFactory.KeyPrefix(RepoId, LiveSpace),
                Does.Not.Contain(LiveSpace.ModelId),
                "the prefix carries a fingerprint of the space, never a model id verbatim");
        });
        Assert.Throws<ArgumentNullException>(
            () => LatticeRepoContextAnnBackingFactory.KeyPrefix(null!, LiveSpace));
    }

    [Test]
    public void A_null_repo_id_is_rejected_by_every_entry_point()
    {
        var factory = Factory(new IndexTree());

        Assert.Multiple(() =>
        {
            Assert.That(() => factory.CreateSource(null!, LiveSpace), Throws.ArgumentNullException);
            Assert.That(() => factory.CreateStore(null!, LiveSpace), Throws.ArgumentNullException);
            Assert.That(
                () => factory.ReclaimSupersededSpacesAsync(null!, LiveSpace, Ct),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task An_empty_index_root_retires_nothing()
    {
        var tree = new IndexTree();

        var retired = await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.Zero);
            Assert.That(tree.DeletedRanges, Is.Empty, "an empty root must issue no range delete at all");
        });
    }

    [Test]
    public async Task Only_the_live_space_present_retires_nothing()
    {
        var tree = new IndexTree();
        tree.PutSpace(RepoId, LiveSpace, 4);

        var retired = await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.Zero);
            Assert.That(tree.DeletedRanges, Is.Empty,
                "THE LIVE INDEX MUST SURVIVE: a range delete in reach of the live prefix destroys it");
            Assert.That(tree.Keys, Has.Count.EqualTo(5));
        });
    }

    [Test]
    public async Task Every_superseded_space_is_retired_and_the_live_one_is_left_whole()
    {
        var tree = new IndexTree();
        tree.PutSpace(RepoId, OldSpace, 3);
        tree.PutSpace(RepoId, OlderSpace, 3);
        tree.PutSpace(RepoId, LiveSpace, 3);

        var retired = await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        var livePrefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, LiveSpace);
        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.EqualTo(2), "both abandoned spaces are retired in one pass");
            Assert.That(
                tree.Keys.All(k => k.StartsWith(livePrefix, StringComparison.Ordinal)), Is.True,
                "nothing but the live space may remain");
            Assert.That(tree.Keys, Has.Count.EqualTo(4));
        });
    }

    [Test]
    public async Task The_walk_skips_a_whole_space_per_read_rather_than_enumerating_its_records()
    {
        // The walk's whole reason for existing: a superseded space holds hundreds
        // of thousands of records, so a per-record enumeration would read the very
        // data the reclamation exists to remove.
        var tree = new IndexTree();
        tree.PutSpace(RepoId, OldSpace, 500);
        tree.PutSpace(RepoId, LiveSpace, 500);

        await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        // One bounded key scan per space observed, not one per record.
        var scans = tree.Tree.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILattice.KeysAsync));
        Assert.That(scans, Is.LessThanOrEqualTo(4),
            $"the skip scan must cost a read per space, not per record (observed {scans})");
    }

    [Test]
    public async Task A_key_under_the_root_that_names_no_space_is_stepped_over_not_deleted()
    {
        // A key sitting directly beneath the repository root carries no fingerprint
        // segment, so it names no space. Treating it as one would compute the root
        // itself as the prefix, and range-deleting that takes every space the
        // repository has - the live index included.
        var tree = new IndexTree();
        var root = RepoContextAnnIndexKeys.RepositoryRoot(RepoId);
        tree.Put(root + "stray-marker");
        tree.PutSpace(RepoId, OldSpace, 2);
        tree.PutSpace(RepoId, LiveSpace, 2);

        var retired = await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.EqualTo(1), "only the real abandoned space is retired");
            Assert.That(tree.Keys, Does.Contain(root + "stray-marker"),
                "a key this plane does not own is stepped past, never deleted");
            Assert.That(
                tree.Keys.Any(k => k.StartsWith(
                    RepoContextAnnIndexKeys.IndexPrefix(RepoId, LiveSpace), StringComparison.Ordinal)),
                Is.True);
        });
    }

    [Test]
    public async Task Another_repository_is_never_in_reach()
    {
        var tree = new IndexTree();
        tree.PutSpace("other", OldSpace, 3);
        tree.PutSpace("other", LiveSpace, 3);
        tree.PutSpace(RepoId, OldSpace, 3);
        tree.PutSpace(RepoId, LiveSpace, 3);

        var retired = await Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        var foreignPrefix = RepoContextAnnIndexKeys.RepositoryRoot("other");
        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.EqualTo(1));
            Assert.That(
                tree.Keys.Count(k => k.StartsWith(foreignPrefix, StringComparison.Ordinal)), Is.EqualTo(8),
                "a repository-scoped walk must never reach another repository's index, whatever its state");
        });
    }

    [Test]
    public void Cancellation_stops_the_walk()
    {
        var tree = new IndexTree();
        tree.PutSpace(RepoId, OldSpace, 3);
        tree.PutSpace(RepoId, LiveSpace, 3);

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            () => Factory(tree).ReclaimSupersededSpacesAsync(RepoId, LiveSpace, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(tree.DeletedRanges, Is.Empty,
            "a cancelled reclamation must not have half-deleted a space");
    }

    [Test]
    public async Task Reclamation_is_idempotent()
    {
        var tree = new IndexTree();
        tree.PutSpace(RepoId, OldSpace, 3);
        tree.PutSpace(RepoId, LiveSpace, 3);

        var factory = Factory(tree);
        var first = await factory.ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);
        var second = await factory.ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1));
            Assert.That(second, Is.Zero, "a second pass finds only the live space and does nothing");
        });
    }

    [Test]
    public void The_source_and_store_bind_to_the_reserved_trees()
    {
        var tree = new IndexTree();
        var factory = Factory(tree);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateSource(RepoId, LiveSpace), Is.Not.Null);
            Assert.That(factory.CreateStore(RepoId, LiveSpace), Is.Not.Null);
        });
        tree.GrainFactory.Received().GetGrain<ILattice>(RepoContextTrees.VectorIndex);
    }
}
