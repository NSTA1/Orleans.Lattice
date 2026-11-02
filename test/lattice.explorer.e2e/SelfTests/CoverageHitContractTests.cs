using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The hit API's contract (#5361): a hit is evidence only when the step that proposed it
/// completes. A page object that records its hit before its assertion is rejected when the
/// assertion fails, because its scope never completes.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class CoverageHitContractTests
{
    private const string Proved = "use-case:selftest/proved";
    private const string Claimed = "use-case:selftest/claimed";
    private const string Earlier = "use-case:selftest/earlier";

    private CoverageRecorder _recorder = null!;

    /// <summary>Creates a recorder over a three-row catalogue, away from the run's evidence; each test routes its hits to it.</summary>
    [SetUp]
    public void CreateRecorder()
    {
        var catalogue = FeatureCatalogue.Parse(
            string.Join('\n',
                "id\tkind\tsource\tdescription\tjourney",
                $"{Proved}\tuse-case\tdocs/lattice.explorer/areas.md#data\tA feature a page object proves\tA1",
                $"{Claimed}\tuse-case\tdocs/lattice.explorer/areas.md#data\tA feature a page object only claims\tA1",
                $"{Earlier}\tuse-case\tdocs/lattice.explorer/areas.md#data\tA feature an earlier step proves\tA1"),
            "selftest.tsv");
        _recorder = new CoverageRecorder(() => catalogue);
    }

    /// <summary>
    /// The page object records its hit, then its assertion fails: the step throws, its scope never
    /// completes, and the hit is discarded - the hit API rejects a claim made before its proof.
    /// </summary>
    [Test]
    public async Task A_hit_recorded_before_a_failing_assertion_is_rejected()
    {
        using var route = CoverageRecorder.Use(_recorder);
        var failure = await CaptureAsync(() => StepAsync("A1.1", ClaimBeforeProvingAsync));

        Assert.Multiple(() =>
        {
            Assert.That(failure, Is.TypeOf<SimulatedAssertionException>(), "the step failed on the page object's assertion");
            Assert.That(_recorder.Hits.Keys, Does.Not.Contain(Claimed), "the claimed hit is not evidence");
            Assert.That(_recorder.Hits, Is.Empty);
        });
    }

    /// <summary>A page object that asserts, then records its hit, commits when its step completes, under the step's name.</summary>
    [Test]
    public async Task A_hit_recorded_after_its_assertion_commits_with_its_step()
    {
        using var route = CoverageRecorder.Use(_recorder);
        await StepAsync("A1.2", ProveThenHitAsync);

        Assert.That(_recorder.Hits[Proved], Is.EqualTo(new[] { "A1.2" }));
    }

    /// <summary>A later step's failure keeps an earlier step's evidence and rejects its own.</summary>
    [Test]
    public async Task A_completed_step_keeps_its_evidence_when_a_later_step_fails()
    {
        using var route = CoverageRecorder.Use(_recorder);
        using (var chapter = Coverage.BeginScope("A1"))
        {
            await StepAsync("A1.1", () =>
            {
                Coverage.Hit(Earlier);
                return Task.CompletedTask;
            });
            var failure = await CaptureAsync(() => StepAsync("A1.2", ClaimBeforeProvingAsync));
            Assert.That(failure, Is.TypeOf<SimulatedAssertionException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(_recorder.Hits.Keys, Is.EqualTo(new[] { Earlier }));
            Assert.That(_recorder.Hits[Earlier], Is.EqualTo(new[] { "A1.1" }));
        });
    }

    /// <summary>A hit outside any scope is refused: there is no step whose completion could prove it.</summary>
    [Test]
    public void A_hit_outside_a_scope_is_refused()
    {
        using var route = CoverageRecorder.Use(_recorder);
        Assert.That(() => Coverage.Hit(Proved), Throws.InvalidOperationException.With.Message.Contains("outside a coverage scope"));
    }

    /// <summary>A hit for a feature the catalogue lacks is refused, so a typo cannot cover nothing.</summary>
    [Test]
    public void A_hit_for_an_unknown_feature_is_refused()
    {
        using var route = CoverageRecorder.Use(_recorder);
        using var scope = Coverage.BeginScope("A1.3");
        Assert.That(() => Coverage.Hit("use-case:selftest/typo"), Throws.ArgumentException.With.Message.Contains("explorer-features.tsv"));
    }

    /// <summary>A hit after its scope completed is refused: it belongs to no step's proof.</summary>
    [Test]
    public void A_hit_after_its_scope_completed_is_refused()
    {
        using var route = CoverageRecorder.Use(_recorder);
        using var scope = Coverage.BeginScope("A1.4");
        scope.Complete();
        Assert.That(() => Coverage.Hit(Proved), Throws.InvalidOperationException.With.Message.Contains("completed"));
    }

    /// <summary>The run's recorder writes coverage-hits.json, which the gate reads back.</summary>
    [Test]
    public async Task Committed_hits_round_trip_through_the_hits_file()
    {
        using var route = CoverageRecorder.Use(_recorder);
        await StepAsync("A1.5", ProveThenHitAsync);
        var directory = Path.Combine(Path.GetTempPath(), "lattice-e2e-hits-" + Guid.NewGuid().ToString("N"));
        try
        {
            _recorder.WriteTo(Path.Combine(directory, "leg-a", CoverageHitsDocument.FileName), "isolated");
            new CoverageRecorder(() => throw new InvalidOperationException("unused")).WriteTo(Path.Combine(directory, "leg-b", CoverageHitsDocument.FileName), "story");

            Assert.That(CoverageHitsDocument.UnionUnder(directory), Is.EquivalentTo(new[] { Proved }));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs <paramref name="body"/> as one step: inside a scope that completes only if the body succeeds.</summary>
    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope(step);
        await body();
        scope.Complete();
    }

    /// <summary>A page object written wrongly: it claims the feature, then its assertion fails.</summary>
    private static async Task ClaimBeforeProvingAsync()
    {
        Coverage.Hit(Claimed);
        await Task.Yield();
        throw new SimulatedAssertionException();
    }

    /// <summary>A page object written rightly: its assertion succeeds, then it records the hit.</summary>
    private static async Task ProveThenHitAsync()
    {
        await Task.Yield();
        Coverage.Hit(Proved);
    }

    private static async Task<Exception?> CaptureAsync(Func<Task> body)
    {
        try
        {
            await body();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private sealed class SimulatedAssertionException() : Exception("The page object's assertion failed.");
}
