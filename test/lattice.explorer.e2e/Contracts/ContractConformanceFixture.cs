using System.Diagnostics;
using System.Text;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The contract conformance self-test (#5364): on one real estate per storyline, every chapter
/// in serial story order is established through its contract and then verified through it, so
/// a seeder and its verifier can never disagree silently. Running the chapters in order on one
/// estate also proves each seeder establishes only its own chapter on top of the ones before
/// it, and each verifier tolerates what the later chapters change. A chapter whose consumes
/// failed is reported blocked, never passed. Results go to the job summary.
/// </summary>
/// <param name="storyline">The storyline this fixture shards.</param>
public abstract class ContractConformanceFixture(string storyline)
{
    private readonly List<(string Chapter, string Outcome, TimeSpan Establish, TimeSpan Verify, int Retries, string? Detail)> _rows = [];
    private readonly HashSet<string> _failed = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _knownDefectBlocks = new(StringComparer.Ordinal);
    private JourneyEstate? _estate;
    private StorySession? _session;

    /// <summary>The storyline.</summary>
    protected string Storyline { get; } = storyline;

    /// <summary>
    /// The chapter before which the Explorer is photographed: the estate then holds every
    /// produce that chapter consumes. <see langword="null"/> takes no photographs.
    /// </summary>
    protected virtual string? SnapshotBefore => null;

    /// <summary>The Explorer pages photographed, in order, as (file name, path).</summary>
    private static readonly (string Name, string Path)[] SnapshotPages =
    [
        ("01-home", "/"),
        ("02-data", "/data"),
        ("03-access", "/access"),
        ("04-schema", "/schema"),
        ("05-backups", "/backups"),
        ("06-apps", "/apps"),
    ];

    /// <summary>Starts the storyline's estate.</summary>
    [OneTimeSetUp]
    public async Task StartEstateAsync()
    {
        _estate = await StoryEstates.StartAsync(Storyline);
        _session = new StorySession(
            Storyline, JourneyRunMode.Isolated, _estate, ChapterContracts.Discovered, StoryPlan.GraphFor(Storyline), _estate.ArtifactsDirectory);
    }

    /// <summary>Writes the conformance table and the ledger, and stops the estate.</summary>
    [OneTimeTearDown]
    public async Task StopEstateAsync()
    {
        var report = new StringBuilder()
            .AppendLine($"### Contract conformance: {Storyline}")
            .AppendLine()
            .AppendLine("| Chapter | Outcome | Establish | Verify | Retries | Detail |")
            .AppendLine("|---|---|---|---|---|---|");
        foreach (var row in _rows)
        {
            report.AppendLine($"| {row.Chapter} | {row.Outcome} | {row.Establish.TotalSeconds:0.0}s | {row.Verify.TotalSeconds:0.0}s | {row.Retries} | {Escape(row.Detail)} |");
        }

        JobSummary.Append(report.ToString());
        if (_session is not null && _estate is not null)
        {
            File.WriteAllText(Path.Combine(_estate.ArtifactsDirectory, $"conformance-ledger-{Storyline}.json"), _session.Ledger.ToJson());
        }

        if (_estate is not null)
        {
            await _estate.DisposeAsync();
        }
    }

    /// <summary>One test case per outline chapter of <paramref name="storyline"/>, in serial story order.</summary>
    /// <param name="storyline">The storyline.</param>
    protected static IEnumerable<TestCaseData> CasesFor(string storyline) =>
        StoryOutline.For(storyline).Select((chapter, index) =>
            new TestCaseData(chapter.Id).SetName($"{index + 1:D2}_{chapter.Id}_establishes_what_it_verifies"));

    /// <summary>Establishes <paramref name="chapter"/> through its contract, then verifies it through the same contract.</summary>
    /// <param name="chapter">The chapter id.</param>
    protected async Task EstablishThenVerifyAsync(string chapter)
    {
        var session = _session ?? throw new InvalidOperationException("The estate did not start.");
        var blockers = StoryOutline.Find(chapter)!.After.Where(_failed.Contains).ToList();
        if (blockers.Count > 0)
        {
            _failed.Add(chapter);
            var issues = blockers.Select(blocker => _knownDefectBlocks.GetValueOrDefault(blocker)).Where(issue => issue is not null).Distinct().ToList();
            var known = issues.Count > 0 && blockers.All(_knownDefectBlocks.ContainsKey);
            if (known)
            {
                _knownDefectBlocks[chapter] = issues[0]!;
            }

            var by = "blocked by " + string.Join(", ", blockers) + (known ? $" (known defect {string.Join(", ", issues)})" : string.Empty);
            _rows.Add((chapter, known ? $"blocked by {string.Join(", ", issues)}" : "blocked", TimeSpan.Zero, TimeSpan.Zero, 0, by));
            Assert.Ignore($"BLOCKED, never passed: {chapter} consumes {string.Join(", ", blockers)}, which did not conform{(known ? $" (known defect {string.Join(", ", issues)})" : string.Empty)}.");
        }

        var retriesBefore = ContractRetries.Count;
        var clock = Stopwatch.StartNew();
        TimeSpan establish = TimeSpan.Zero;
        if (string.Equals(chapter, SnapshotBefore, StringComparison.Ordinal))
        {
            await SnapshotEstateAsync(chapter);
        }

        try
        {
            await session.EstablishAsync(chapter);
            establish = clock.Elapsed;
            clock.Restart();
            await session.VerifyAsync(chapter);
            _rows.Add((chapter, "conforms", establish, clock.Elapsed, ContractRetries.Count - retriesBefore, null));
        }
        catch (Exception ex) when (ex is not IgnoreException)
        {
            _failed.Add(chapter);
            var phase = establish == TimeSpan.Zero ? "establish failed" : "verify failed";
            var elapsedEstablish = establish == TimeSpan.Zero ? clock.Elapsed : establish;
            var elapsedVerify = establish == TimeSpan.Zero ? TimeSpan.Zero : clock.Elapsed;
            var retries = ContractRetries.Count - retriesBefore;
            if (KnownDefectIssues.TryGetValue(chapter, out var known))
            {
                // An open epic sub-issue tracks this chapter's failure: report it, with the
                // estate's state, and block its dependents, without failing the job. Any other
                // failure stays red. I1 (#5400) requires every such entry gone.
                var state = await DescribeKnownDefectStateAsync(chapter);
                _knownDefectBlocks[chapter] = known;
                await RecordKnownDefectAsync(chapter);
                _rows.Add((chapter, $"known defect {known}", elapsedEstablish, elapsedVerify, retries, $"{phase}: {FirstLine(ex)} State: {state}"));
                Assert.Ignore($"KNOWN DEFECT {known}: {chapter} {phase}: {FirstLine(ex)} State: {state}");
            }

            _rows.Add((chapter, phase, elapsedEstablish, elapsedVerify, retries, FirstLine(ex)));
            throw;
        }
    }

    /// <summary>
    /// The chapters whose failure an open epic sub-issue tracks, as chapter to issue: such a
    /// chapter is reported, its dependents are reported blocked by the issue, and neither fails
    /// the job.
    /// </summary>
    protected virtual IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Records the pending wrapper for a known-defect chapter that failed (a <c>KnownDefect</c>
    /// written with literals, as the pending-wrapper gate requires).
    /// </summary>
    /// <param name="chapter">The chapter.</param>
    protected virtual Task RecordKnownDefectAsync(string chapter) => Task.CompletedTask;

    /// <summary>The estate's state that bears on a known-defect chapter's failure, for the report.</summary>
    /// <param name="chapter">The chapter.</param>
    protected virtual Task<string> DescribeKnownDefectStateAsync(string chapter) => Task.FromResult("n/a");

    /// <summary>The running estate.</summary>
    protected JourneyEstate Estate => _estate ?? throw new InvalidOperationException("The estate did not start.");

    /// <summary>
    /// Photographs the Explorer, signed in as the storyline's first bootstrap administrator, on
    /// the estate as established so far, into <c>artifacts/screenshots/contract-conformance-*</c>.
    /// Best effort: a failed photograph is reported, never fails the self-test.
    /// </summary>
    /// <param name="chapter">The chapter about to be established.</param>
    private async Task SnapshotEstateAsync(string chapter)
    {
        var estate = _estate ?? throw new InvalidOperationException("The estate did not start.");
        var directory = Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "screenshots", $"contract-conformance-{Storyline.ToLowerInvariant()}");
        Directory.CreateDirectory(directory);
        try
        {
            var persona = Personas.BootstrapAdministratorsOf(Storyline.ToLowerInvariant()).Select(Personas.Get).First();
            await using var session = await estate.SignInAsAsync(persona);
            foreach (var (name, path) in SnapshotPages)
            {
                await session.Shell.GotoAsync(path);
                await Task.Delay(TimeSpan.FromSeconds(3));
                await session.Page.ScreenshotAsync(new() { Path = Path.Combine(directory, $"{name}.png"), FullPage = true });
            }

            TestContext.Progress.WriteLine($"Photographed the {Storyline} estate at {chapter}'s consumes into {directory}.");
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Could not photograph the {Storyline} estate at {chapter}'s consumes: {FirstLine(ex)}");
        }
    }

    private static string FirstLine(Exception exception)
    {
        var message = exception is AggregateException { InnerException: { } inner } ? inner.Message : exception.Message;
        var line = message.Split('\n', 2)[0].Trim();
        return line.Length > 240 ? line[..240] + "..." : line;
    }

    private static string Escape(string? text) => text is null ? string.Empty : text.Replace("|", "\\|", StringComparison.Ordinal);
}
