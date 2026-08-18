namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the reuse-economics surface of <see cref="RepoContextBundleService"/>:
/// per-unit opaque receipts, whole-file possession suppression, cross-call session
/// bookkeeping, and the load-bearing partial-to-whole possession guard. They drive the
/// real service over substituted trees (a mutable session tree so bookkeeping persists
/// across calls) and assert on delivered content, acknowledgements, and the invariant
/// that a reused unit is never charged against the token budget or the file count.
/// </summary>
public sealed partial class RepoContextBundleServiceTests
{
    private const string ReuseBody =
        "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle reuse";

    [Test]
    public async Task Build_without_reuse_inputs_leaves_units_and_hash_empty()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, ReuseBody, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Session, Is.Null);
            Assert.That(result.Reused, Is.Empty);
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].ContentHash, Is.Null,
                "The non-engaged path must not compute a content hash, preserving the original behaviour.");
            Assert.That(result.Entries[0].Units, Is.Empty,
                "The non-engaged path must not emit reusable units.");
        });
    }

    [Test]
    public async Task Build_with_engaged_reuse_emits_stable_receipts_and_hash()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, ReuseBody, 4096L));

        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-stable", CancellationToken.None);

        var fresh = BuildService((path, ReuseBody, 4096L));
        var second = await fresh.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-stable-2", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries, Has.Count.EqualTo(1));
            Assert.That(first.Entries[0].Units, Has.Count.EqualTo(1));
            Assert.That(first.Entries[0].ContentHash, Is.Not.Null.And.Not.Empty);
            Assert.That(first.Entries[0].Units[0].Kind, Is.EqualTo("span"));
            // A receipt and hash are pure functions of their inputs, so a second service
            // over the same file yields byte-identical tokens.
            Assert.That(second.Entries[0].ContentHash, Is.EqualTo(first.Entries[0].ContentHash));
            Assert.That(second.Entries[0].Units[0].Receipt, Is.EqualTo(first.Entries[0].Units[0].Receipt));
        });
    }

    [Test]
    public async Task Build_suppresses_exactly_the_seen_unit_and_still_delivers_the_rest()
    {
        const string path = "src/Widget.cs";
        var service = BuildServiceWithSymbols(
            path, ReuseBody, 4096L,
            ("Acme.Widget.Alpha", "public void Alpha()"),
            ("Acme.Widget.Beta", "public void Beta()"));

        // First call engages reuse (empty session) purely to learn the per-symbol receipts.
        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline,
            seen: null, known: null, session: "s-units", CancellationToken.None);
        var units = first.Entries[0].Units;
        Assert.That(units, Has.Count.EqualTo(2), "The file declares two symbols, so outline delivers two units.");
        var alpha = units.Single(u => u.Symbol == "Acme.Widget.Alpha");
        var beta = units.Single(u => u.Symbol == "Acme.Widget.Beta");

        // Second call: hand back exactly Alpha's receipt via `seen`. Beta must still arrive.
        var fresh = BuildServiceWithSymbols(
            path, ReuseBody, 4096L,
            ("Acme.Widget.Alpha", "public void Alpha()"),
            ("Acme.Widget.Beta", "public void Beta()"));
        var second = await fresh.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline,
            seen: [alpha.Receipt], known: null, session: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Has.Count.EqualTo(1));
            var entry = second.Entries[0];
            Assert.That(entry.Units, Has.Count.EqualTo(1));
            Assert.That(entry.Units[0].Symbol, Is.EqualTo("Acme.Widget.Beta"));
            Assert.That(entry.Content, Does.Contain("Beta").And.Not.Contain("Alpha"),
                "Only the surviving unit's content is delivered.");
            // The reused unit is acknowledged and never charged.
            Assert.That(second.Reused, Has.Count.EqualTo(1));
            Assert.That(second.Reused[0].Kind, Is.EqualTo("outline"));
            Assert.That(second.Reused[0].Receipt, Is.EqualTo(alpha.Receipt));
            Assert.That(second.Reused[0].Symbol, Is.EqualTo("Acme.Widget.Alpha"));
            Assert.That(second.TotalTokens, Is.EqualTo(entry.TokenCount));
            Assert.That(entry.TokenCount, Is.EqualTo(Counter.CountTokens(entry.Content)));
            _ = beta;
        });
    }

    [Test]
    public async Task Build_suppresses_the_whole_file_when_a_known_possession_validates()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, ReuseBody, 4096L));

        // Call 1 at slices delivers the whole body and records possession in session "s".
        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-file", CancellationToken.None);
        var hash = first.Entries[0].ContentHash!;
        Assert.That(hash, Is.Not.Null);

        // Call 2 asserts whole-file possession of that version and asks for a cheaper
        // projection (outline); possession makes any projection redundant.
        var second = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline,
            seen: null, known: [$"{path}@{hash}"], session: "s-file", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Is.Empty, "The possessed file consumes no delivery slot.");
            Assert.That(second.TotalTokens, Is.EqualTo(0));
            Assert.That(second.Reused, Has.Count.EqualTo(1));
            Assert.That(second.Reused[0].Kind, Is.EqualTo("file"));
            Assert.That(second.Reused[0].Path, Is.EqualTo(path));
            Assert.That(second.Reused[0].ContentHash, Is.EqualTo(hash));
            Assert.That(second.Reused[0].Receipt, Is.Null, "A whole-file ack is matched by possession, not by receipt.");
        });
    }

    [Test]
    public async Task Build_auto_suppresses_a_unit_a_prior_call_recorded_in_the_same_session()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, ReuseBody, 4096L));

        // Call 1 delivers the span and records its receipt into session "s".
        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-persist", CancellationToken.None);
        Assert.That(first.Entries, Has.Count.EqualTo(1));

        // Call 2 supplies neither seen nor known; the session alone must suppress it.
        var second = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-persist", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Is.Empty, "The session already delivered this unit.");
            Assert.That(second.TotalTokens, Is.EqualTo(0));
            Assert.That(second.Reused, Has.Count.EqualTo(1));
            Assert.That(second.Reused[0].Kind, Is.EqualTo("span"));
            Assert.That(second.Session, Is.EqualTo("s-persist"));
        });
    }

    [Test]
    public async Task Build_never_promotes_partial_evidence_to_whole_file_possession()
    {
        const string path = "src/Widget.cs";
        var service = BuildServiceWithSymbols(
            path, ReuseBody, 4096L,
            ("Acme.Widget.Alpha", "public void Alpha()"),
            ("Acme.Widget.Beta", "public void Beta()"));

        // Call 1 delivers only an OUTLINE (partial evidence) into session "s"; this must
        // record unit receipts but never a whole-file possession token.
        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline,
            seen: null, known: null, session: "s-guard", CancellationToken.None);
        Assert.That(first.Entries, Has.Count.EqualTo(1));

        // The caller now forges a whole-file possession claim for that version and asks
        // for slices. The guard must reject it: the file is delivered in full.
        var hash = RepoContextReuse.ContentHash(StoredBody(path, ReuseBody));
        var second = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: [$"{path}@{hash}"], session: "s-guard", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Has.Count.EqualTo(1),
                "A version only ever delivered as an outline can never satisfy a whole-file claim.");
            Assert.That(second.Entries[0].Path, Is.EqualTo(path));
            Assert.That(second.Reused.Any(r => r.Kind == "file"), Is.False,
                "No whole-file suppression may occur off partial evidence.");
        });
    }

    [Test]
    public async Task Build_fails_closed_on_a_known_claim_with_no_session_oracle()
    {
        const string path = "src/Widget.cs";
        var service = BuildService((path, ReuseBody, 4096L));

        var hash = RepoContextReuse.ContentHash(StoredBody(path, ReuseBody));
        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: [$"{path}@{hash}"], session: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1),
                "Without a session there is no possession oracle, so a known claim can never validate.");
            Assert.That(result.Reused.Any(r => r.Kind == "file"), Is.False);
        });
    }

    [Test]
    public async Task Build_backfills_a_fresh_file_past_a_fully_reused_one_without_spending_the_top_slot()
    {
        const string first = "src/First.cs";
        const string second = "src/Second.cs";
        const string firstBody = "namespace Acme; public sealed class First { } // widget lattice bundle one";
        const string secondBody = "namespace Acme; public sealed class Second { } // widget lattice bundle two";
        var service = BuildService((first, firstBody, 100L), (second, secondBody, 100L));

        // Call 1 delivers both files (top = 2) and records both spans into the session.
        var seed = await service.BuildAsync(
            RepoId, "widget bundle", 2, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-backfill", CancellationToken.None);
        Assert.That(seed.Entries, Has.Count.EqualTo(2));
        var topRanked = seed.Entries[0].Path;
        var topSpan = seed.Entries[0].Units[0].Receipt;
        var runnerUp = seed.Entries[1].Path;

        // Call 2 with top = 1 hands back the top-ranked file's span. Backfill must still
        // deliver the runner-up rather than returning an empty bundle.
        var fresh = BuildService((first, firstBody, 100L), (second, secondBody, 100L));
        var result = await fresh.BuildAsync(
            RepoId, "widget bundle", 1, 10_000, RepoContextContextDetail.Slices,
            seen: [topSpan], known: null, session: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].Path, Is.EqualTo(runnerUp),
                "The fully-reused top-ranked file did not consume the single delivery slot.");
            Assert.That(result.Reused.Any(r => r.Path == topRanked && r.Kind == "span"), Is.True);
        });
    }

    [Test]
    public void Build_rejects_null_repo_or_task_on_the_reuse_overload()
    {
        var service = BuildService(("src/Widget.cs", ReuseBody, 4096L));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await service.BuildAsync(
                    null!, "t", 10, 100, RepoContextContextDetail.Auto,
                    seen: null, known: null, session: "s", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await service.BuildAsync(
                    RepoId, null!, 10, 100, RepoContextContextDetail.Auto,
                    seen: null, known: null, session: "s", CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }
}
