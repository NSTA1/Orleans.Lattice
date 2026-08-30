namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the usage recording <see cref="RepoContextBundleService"/> performs on every
/// answered <c>repocontext_context</c> call. They prove the figures are computed conservatively from
/// the returned bundle, and - critically - that recording is side-effect-free: the answer a call
/// returns is byte-for-byte the answer it would return with recording disabled.
/// </summary>
public sealed partial class RepoContextBundleServiceTests
{
    [Test]
    public async Task Build_records_one_call_with_exact_response_and_slices_credit()
    {
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";
        var recorder = new CapturingUsageRecorder();
        var service = BuildService(recorder, (path, body, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.That(recorder.Recorded, Has.Count.EqualTo(1), "Exactly one answered call is recorded.");
        var usage = recorder.Recorded[0];
        Assert.Multiple(() =>
        {
            Assert.That(usage.Command, Is.EqualTo("repocontext_context"));
            Assert.That(usage.ResponseTokens, Is.EqualTo(result.ResponseTokens),
                "The recorded cost is what the caller actually received - envelope and dual emission included - "
                + "not merely the source text inside it (#1811).");
            Assert.That(usage.ResponseTokens, Is.GreaterThan(result.TotalTokens),
                "The wire cost always exceeds the content total, so recording the content total under-reports it.");
            Assert.That(usage.ReplacedReadTokens, Is.EqualTo(4096),
                "A slices delivery credits the whole-file read cost it replaced.");
        });
    }

    [Test]
    public async Task Build_paths_detail_records_zero_read_replacement_credit()
    {
        const string path = "src/Widget.cs";
        var recorder = new CapturingUsageRecorder();
        var service = BuildService(recorder, (path, "class Widget { } // widget", 4096L));

        _ = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Paths, CancellationToken.None);

        Assert.That(recorder.Recorded, Has.Count.EqualTo(1));
        Assert.That(recorder.Recorded[0].ReplacedReadTokens, Is.Zero,
            "Paths detail is a pointer, not a whole-file replacement, so it earns no credit.");
    }

    [Test]
    public async Task Build_with_no_matching_source_records_a_zero_cost_zero_credit_call()
    {
        var recorder = new CapturingUsageRecorder();
        var service = BuildService(recorder);

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.That(result.Entries, Is.Empty);
        Assert.That(recorder.Recorded, Has.Count.EqualTo(1), "An empty answer is still an answered call.");
        Assert.Multiple(() =>
        {
            Assert.That(recorder.Recorded[0].ResponseTokens, Is.Zero);
            Assert.That(recorder.Recorded[0].ReplacedReadTokens, Is.Zero);
        });
    }

    [Test]
    public async Task Build_recording_never_changes_the_answer()
    {
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";

        // The same inputs bundled once with recording disabled and once with a capturing recorder must
        // yield an identical answer - recording is a pure side channel over the returned bundle.
        var quiet = await BuildService(NoOpUsageRecorder.Instance, (path, body, 4096L)).BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        var recorder = new CapturingUsageRecorder();
        var recorded = await BuildService(recorder, (path, body, 4096L)).BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Detail, Is.EqualTo(quiet.Detail));
            Assert.That(recorded.TotalTokens, Is.EqualTo(quiet.TotalTokens));
            Assert.That(recorded.Truncated, Is.EqualTo(quiet.Truncated));
            Assert.That(recorded.RetryBudgetTokens, Is.EqualTo(quiet.RetryBudgetTokens));
            Assert.That(recorded.Entries.Count, Is.EqualTo(quiet.Entries.Count));
            for (var i = 0; i < recorded.Entries.Count; i++)
            {
                Assert.That(recorded.Entries[i].Path, Is.EqualTo(quiet.Entries[i].Path));
                Assert.That(recorded.Entries[i].Content, Is.EqualTo(quiet.Entries[i].Content));
                Assert.That(recorded.Entries[i].TokenCount, Is.EqualTo(quiet.Entries[i].TokenCount));
                Assert.That(recorded.Entries[i].FullReadTokenCount, Is.EqualTo(quiet.Entries[i].FullReadTokenCount));
            }
        });
    }

    [Test]
    public async Task Build_records_figures_matching_the_returned_bundle()
    {
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";
        var recorder = new CapturingUsageRecorder();
        var service = BuildService(recorder, (path, body, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        // The recorded figures are exactly what the conservative computer derives from the returned
        // bundle - the call observed the same immutable result it handed back.
        var expected = RepoContextUsageFigures.ForContextBundle(result);
        Assert.That(recorder.Recorded[0], Is.EqualTo(expected));
    }
}
