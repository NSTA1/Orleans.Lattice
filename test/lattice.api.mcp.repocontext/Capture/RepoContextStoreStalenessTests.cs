using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for memory-link staleness detection on
/// <see cref="RepoContextStore"/>. When a memory entry links to a structural
/// target, the store captures the target's content digest at link time; a later
/// evaluating recall (and each neighbor of a knowledge-graph walk) compares that
/// captured digest against the target's current digest and reports drift through
/// <see cref="RepoContextEntryView.Stale"/> and
/// <see cref="RepoContextEntryView.StaleLinks"/>. A non-evaluating recall leaves
/// those fields <see langword="null"/> ("not evaluated"), mirroring the expiry
/// convention.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreStalenessTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Tree(RepoContextMcpHarness harness, string treeName)
        => harness.GrainFactory.GetGrain<ILattice>(treeName);

    private static async Task SeedFileAsync(
        RepoContextMcpHarness harness, string repoId, string path, string digest, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<FileNode>>();
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var node = new FileNode { RepoId = repoId, Path = path, Digest = RepoContextValues.Lww(digest, clock) };
        await Tree(harness, RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.File(repoId, path), serializer.SerializeToArray(node), ct);
    }

    private static async Task<string> RememberLinkedAsync(
        RepoContextStore store, string repoId, string fileKey, CancellationToken ct)
    {
        var result = await store.RememberAsync(
            repoId, "glossary", id: null, MemoryKind.Note, title: "linked", body: null,
            author: null, provenance: null, tags: null,
            addLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { fileKey } },
            removeLinks: null, ttlSeconds: null, ct);
        return result.Key;
    }

    /// <summary>
    /// Reads the digest the store captured for <paramref name="target"/> when the
    /// edge was written, straight from the stored memory record, so a fixture can
    /// assert its own arrange state rather than assume it. Returns
    /// <see langword="null"/> when no digest was captured for that target.
    /// </summary>
    private static async Task<string?> CapturedDigestAsync(
        RepoContextMcpHarness harness, string memKey, string target, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var stored = await Tree(harness, RepoContextTrees.Memory).GetAsync(memKey, ct);
        var record = RepoContextMemoryCodec.Fold(stored, serializer);
        Assert.That(record, Is.Not.Null, "The memory entry must exist for its arrange state to be meaningful.");
        var register = record!.LinkDigests.Get(target);
        return register is null ? null : RepoContextValues.ReadString(register);
    }

    private static async Task<string> RememberWithLinksAsync(
        RepoContextStore store, string repoId,
        IReadOnlyDictionary<string, IReadOnlyList<string>> links, CancellationToken ct)
    {
        var result = await store.RememberAsync(
            repoId, "glossary", id: null, MemoryKind.Note, title: "linked", body: null,
            author: null, provenance: null, tags: null,
            addLinks: links, removeLinks: null, ttlSeconds: null, ct);
        return result.Key;
    }

    private static async Task<string> RememberLinkedManyAsync(
        RepoContextStore store, string repoId, IReadOnlyList<string> targets, CancellationToken ct)
    {
        var result = await store.RememberAsync(
            repoId, "glossary", id: null, MemoryKind.Note, title: "linked", body: null,
            author: null, provenance: null, tags: null,
            addLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = targets },
            removeLinks: null, ttlSeconds: null, ct);
        return result.Key;
    }

    [Test]
    public async Task Recall_reports_a_link_as_fresh_when_the_target_digest_is_unchanged()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False, "The captured digest still matches the target.");
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }

    [Test]
    public async Task Recall_flags_a_link_as_stale_after_the_target_digest_drifts()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // The target file changes on disk: its content digest is re-projected.
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "The target digest drifted since the link was made.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    [Test]
    public async Task Recall_flags_a_link_as_stale_when_the_target_is_deleted()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // The target file is removed entirely: it no longer carries any digest.
        await Tree(harness, RepoContextTrees.Structural).DeleteAsync(fileKey, Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "A vanished target is drift.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    [Test]
    public async Task Non_evaluating_recall_leaves_staleness_unevaluated()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.Null, "A bulk-convention recall does not evaluate staleness.");
            Assert.That(view.StaleLinks, Is.Null);
        });
    }

    [Test]
    public async Task Recall_ignores_the_captured_digest_of_an_unlinked_target()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // Remove the edge, then drift the target: with no live link, the lingering
        // captured digest must not produce a phantom stale flag.
        await store.UpdateAsync(
            memKey, fields: null, addTags: null, removeTags: null, addLinks: null,
            removeLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { fileKey } }, Ct);
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False, "An unlinked target is not evaluated for staleness.");
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }

    [Test]
    public async Task Recall_of_a_memory_entry_without_links_reports_not_stale()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "notes", id: null, MemoryKind.Note, title: "plain", body: "no links",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        var view = await store.RecallAsync(result.Key, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False);
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
        });
    }

    // ---------------------------------------------------------------------
    // Issue #2654: a link written against a target that is absent from the
    // corpus captures no digest, so the digest comparison never runs. Before
    // the fix that non-result was rendered as the healthy value - byte-identical
    // to the answer an in-sync link gives - so the read path whose documented
    // job is to report drift manufactured confidence instead of withholding it.
    // ---------------------------------------------------------------------

    [Test]
    public async Task Recall_flags_a_link_as_stale_when_the_target_was_never_indexed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        // The target is never seeded: this is a note about code that has not
        // reached the indexed branch, the highest-value link an agent creates.
        var fileKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // Arrange-state proof: no digest was captured, so freshness cannot have
        // been established by a comparison that happened to match.
        Assert.That(
            await CapturedDigestAsync(harness, memKey, fileKey, Ct),
            Is.Null,
            "The link must genuinely carry no captured digest for this case to be the one under test.");

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "A link pointing at nothing is not healthy.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    /// <summary>
    /// The dangling guard. Perturbing the <c>Exists</c> branch of
    /// <c>EvaluateStalenessAsync</c> must redden this and not the deferred guard
    /// below, so neither assertion can shadow the other.
    /// </summary>
    [Test]
    public async Task Recall_reports_a_never_indexed_target_in_dangling_links()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var fileKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.That(
            view.DanglingLinks,
            Is.EqualTo(new[] { fileKey }),
            "'points at nothing' and 'content drifted' have opposite remedies and must be distinguishable.");
    }

    /// <summary>
    /// The deferred guard. A dangling target that later gets indexed leaves the
    /// link with a live target and no digest ever captured - a state the old
    /// code skipped silently, reverting the entry to a healthy-looking read
    /// forever. Perturbing the captured-is-null branch must redden this and not
    /// the dangling guard above.
    /// </summary>
    [Test]
    public async Task Recall_flags_a_link_whose_target_gained_a_record_without_a_captured_digest()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var fileKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);

        // Arrange-state proof, taken before the target appears: without it, "no
        // digest captured" would be indistinguishable from "a digest was captured
        // and happens to match", and the assertions below could not say which
        // state they observed.
        Assert.That(
            await CapturedDigestAsync(harness, memKey, fileKey, Ct),
            Is.Null,
            "The edge must have been written while the target was absent.");

        // The branch merges and the file is indexed for the first time.
        await SeedFileAsync(harness, "acme", "src/Unmerged.cs", "digest-1", Ct);

        Assert.That(
            await CapturedDigestAsync(harness, memKey, fileKey, Ct),
            Is.Null,
            "Indexing the target does not retroactively capture a digest for an existing edge.");

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True, "A link whose drift was never measurable is not evidence of freshness.");
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
            Assert.That(
                view.DanglingLinks,
                Is.Null.Or.Empty,
                "The target now has a live record, so it is not dangling.");
        });
    }

    /// <summary>
    /// Constraint on the shape of the new field: <c>danglingLinks</c> is a strict
    /// subset of <c>staleLinks</c>, never a sibling that partitions it. Were
    /// dangling targets to move out of <c>staleLinks</c>, every caller reading
    /// only <c>staleLinks</c> would silently lose coverage on upgrade - a
    /// regression that presents as an improvement.
    /// </summary>
    [Test]
    public async Task Recall_reports_dangling_links_as_a_subset_of_stale_links()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var driftedKey = RepoContextKeys.File("acme", "src/Drifted.cs");
        var danglingKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        await SeedFileAsync(harness, "acme", "src/Drifted.cs", "digest-1", Ct);

        var memKey = await RememberLinkedManyAsync(store, "acme", new[] { driftedKey, danglingKey }, Ct);
        await SeedFileAsync(harness, "acme", "src/Drifted.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True);
            Assert.That(
                view.StaleLinks,
                Is.EqualTo(new[] { driftedKey, danglingKey }.OrderBy(k => k, StringComparer.Ordinal).ToArray()),
                "Both states are reported through the summary field.");
            Assert.That(view.DanglingLinks, Is.EqualTo(new[] { danglingKey }));
            Assert.That(
                view.DanglingLinks!,
                Is.SubsetOf(view.StaleLinks!),
                "Every dangling link must also be a stale link.");
        });
    }

    [Test]
    public async Task Recall_does_not_report_a_drifted_target_as_dangling()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-2", Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
            Assert.That(
                view.DanglingLinks,
                Is.Null.Or.Empty,
                "The target exists; it is drift, and the remedy is to re-read it.");
        });
    }

    [Test]
    public async Task Recall_reports_a_deleted_target_as_dangling()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var fileKey = RepoContextKeys.File("acme", "src/A.cs");
        await SeedFileAsync(harness, "acme", "src/A.cs", "digest-1", Ct);

        var memKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        await Tree(harness, RepoContextTrees.Structural).DeleteAsync(fileKey, Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.True);
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
            Assert.That(
                view.DanglingLinks,
                Is.EqualTo(new[] { fileKey }),
                "A deleted target points at nothing, exactly as a never-indexed one does: "
                + "the answer follows present state, not unobservable link-time history.");
        });
    }

    [Test]
    public async Task Recall_ignores_a_memory_link_whose_target_has_no_live_entry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        // A memory-to-memory edge is not a structural target: it carries no
        // digest by design, so it is outside the staleness measurand entirely.
        var memoryTarget = RepoContextKeys.Memory("acme", "glossary", "absent");
        var memKey = await RememberLinkedAsync(store, "acme", memoryTarget, Ct);

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Stale, Is.False, "A memory-to-memory edge is never evaluated for drift.");
            Assert.That(view.StaleLinks, Is.Null.Or.Empty);
            Assert.That(view.DanglingLinks, Is.Null.Or.Empty);
        });
    }

    [Test]
    public async Task Neighbors_flags_a_dangling_structural_link_on_a_walked_entry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var fileKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        var leafKey = await RememberLinkedAsync(store, "acme", fileKey, Ct);
        var seedKey = await RememberLinkedAsync(store, "acme", leafKey, Ct);

        var result = await store.NeighborsAsync(seedKey, relation: null, depth: 1, maxNodes: 10, Ct);
        var leaf = result.Neighbors.Single(n => string.Equals(n.Key, leafKey, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Stale, Is.True);
            Assert.That(leaf.DanglingLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    /// <summary>
    /// Pins the dedupe conjunct. The walk is driven by the live link set, where one
    /// target can legitimately appear under several relations; without the ordinal
    /// <c>seen</c> set it would be reported once per relation. Perturbation arm A7
    /// (drop the dedupe) is red on this fixture and green without it.
    /// </summary>
    [Test]
    public async Task Recall_reports_a_target_linked_under_two_relations_exactly_once()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var fileKey = RepoContextKeys.File("acme", "src/Unmerged.cs");
        var memKey = await RememberWithLinksAsync(
            store, "acme",
            new Dictionary<string, IReadOnlyList<string>>
            {
                ["related"] = new[] { fileKey },
                ["broader"] = new[] { fileKey },
            },
            Ct);

        var stored = await store.RecallAsync(memKey, evaluateStaleness: false, Ct);
        Assert.That(
            stored.Links.Count, Is.EqualTo(2),
            "The arrange state must really carry the same target under two relations.");

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.StaleLinks, Is.EqualTo(new[] { fileKey }));
            Assert.That(view.DanglingLinks, Is.EqualTo(new[] { fileKey }));
        });
    }

    /// <summary>
    /// Pins the ordinal ordering conjunct. The three paths are chosen so that their
    /// ordinal order (<c>Z</c> 0x5A, <c>_</c> 0x5F, <c>a</c> 0x61) differs from both
    /// their insertion order and their case-insensitive order, so a fixture that
    /// merely happened to match insertion order could not pass. Perturbation arm A8
    /// (drop the sort) is red on this fixture and green without it.
    /// </summary>
    [Test]
    public async Task Recall_orders_stale_and_dangling_links_ordinally()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var upper = RepoContextKeys.File("acme", "src/Zebra.cs");
        var under = RepoContextKeys.File("acme", "src/_under.cs");
        var lower = RepoContextKeys.File("acme", "src/apple.cs");
        var expected = new[] { upper, under, lower };

        Assert.That(
            expected, Is.Not.EqualTo(expected.OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray()),
            "The fixture must discriminate ordinal ordering from case-insensitive ordering.");

        // The targets are split across two relations, because the unsorted walk
        // order is relation-major: within one relation the link set already
        // enumerates ordinally, so a single-relation fixture cannot tell a sorted
        // result from an unsorted one.
        var memKey = await RememberWithLinksAsync(
            store, "acme",
            new Dictionary<string, IReadOnlyList<string>>
            {
                ["related"] = new[] { upper, lower },
                ["broader"] = new[] { under },
            },
            Ct);

        var walkOrder = (await store.RecallAsync(memKey, evaluateStaleness: false, Ct))
            .Links.SelectMany(pair => pair.Value).ToArray();
        Assert.That(
            walkOrder, Is.Not.EqualTo(expected),
            "The arrange state must really present the links in a non-ordinal walk order.");

        var view = await store.RecallAsync(memKey, evaluateStaleness: true, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.StaleLinks, Is.EqualTo(expected));
            Assert.That(view.DanglingLinks, Is.EqualTo(expected));
        });
    }
}
