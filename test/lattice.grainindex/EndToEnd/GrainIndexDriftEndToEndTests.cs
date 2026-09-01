using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Lattice.GrainIndex.Tests.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.EndToEnd;

/// <summary>
/// The two ends of the drift gate, end to end against a live, populated index:
/// a breaking declaration change is rejected without disturbing what the index
/// already holds, and the same change under
/// <see cref="GrainIndexDriftPolicy.Rebuild"/> is accepted, restarts the crawl,
/// and converges the index onto the new declaration's projection.
/// </summary>
/// <remarks>
/// <para>
/// A restart is modelled by reconciling a declaration set against the same
/// durable registry tree the running silo reconciled at start, which is exactly
/// what a silo coming back up with an edited declaration does. Tearing the
/// cluster down instead would take the in-memory grain storage - and therefore
/// the index itself - with it, leaving nothing to assert about.
/// </para>
/// <para>
/// Each test starts from an index that was genuinely built under a
/// <b>superseded declaration</b>: its entries and its enrolment markers are
/// written through the real projector, the real plan applier, and the real
/// enrolment store, under a declaration that projects a property the live one
/// does not. That is what makes "the old shape is gone" an assertion with
/// something to prove rather than a tautology.
/// </para>
/// <para>
/// Nothing here waits on wall-clock time. The crawl's reminder and timer are off
/// and every pass is invoked explicitly, with the pass count bounded by the
/// population size.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class GrainIndexDriftEndToEndTests
{
    private const string Index = GrainIndexEndToEndClusterFixture.DriftIndex;
    private const int Population = GrainIndexEndToEndClusterFixture.PopulationSize;
    private const int EntriesPerGrain = GrainIndexEndToEndClusterFixture.EntriesPerGrain;

    private GrainIndexEndToEndClusterFixture _fixture = null!;
    private EndToEndPerson[] _people = [];
    private int _runId;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrainIndexEndToEndClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public async Task SetUp()
    {
        _people = GrainIndexEndToEndClusterFixture.Population($"drift{++_runId}");
        await _fixture.ResetIndexAsync(Index);
        await _fixture.SeedDormantDriftAsync(_people);
        await BuildIndexUnderSupersededDeclarationAsync();
    }

    /// <summary>
    /// The declaration the running silo holds: the one a rebuild must converge
    /// the index onto.
    /// </summary>
    private static void DeclareLiveShape(StubSiloBuilder builder) =>
        builder.AddGrainIndex<IEndToEndDriftUserGrain, EndToEndUserState>(
            static cfg => cfg
                .WithName(Index)
                .Include(x => x.Age)
                .Include(x => x.Country));

    /// <summary>
    /// The declaration the index's stored entries were written under, which
    /// projects a property the live one does not.
    /// </summary>
    private static void DeclareSupersededShape(StubSiloBuilder builder) =>
        builder.AddGrainIndex<IEndToEndDriftUserGrain, EndToEndUserState>(
            static cfg => cfg
                .WithName(Index)
                .Include(x => x.Age)
                .Include(x => x.Nickname));

    /// <summary>
    /// Runs the real reconciler over <paramref name="declare"/> against the live
    /// registry tree, which is the work a silo start does for the declaration
    /// set it is holding.
    /// </summary>
    private async Task ReconcileAsync(Action<StubSiloBuilder> declare, GrainIndexDriftPolicy policy)
    {
        var builder = new StubSiloBuilder();
        declare(builder);
        builder.ConfigureGrainIndex(Index, options => options.DriftPolicy = policy);
        builder.Services.AddOptions();
        await using var provider = builder.BuildServiceProvider();

        await new GrainIndexRegistryReconciler(
                provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
                provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
                _fixture.SiloServices.GetRequiredService<IGrainIndexRegistryStore>(),
                new CapturingLogger<GrainIndexRegistryReconciler>(),
                mergeModeResolver: null)
            .ReconcileAsync(CancellationToken.None);
    }

    /// <summary>
    /// Puts the index into the state a previous silo running the superseded
    /// declaration would have left it in: the stored record describes that
    /// declaration, and every grain's entries and enrolment marker were written
    /// from it by the real machinery.
    /// </summary>
    private async Task BuildIndexUnderSupersededDeclarationAsync()
    {
        await ReconcileAsync(DeclareSupersededShape, GrainIndexDriftPolicy.Rebuild);

        var projector = new GrainIndexProjector<IEndToEndDriftUserGrain, EndToEndUserState>(
            GrainIndexEndToEndClusterFixture.SupersededDefinition());
        var enrollments = _fixture.SiloServices.GetRequiredService<IGrainIndexEnrollmentStore>();
        var indexTag = GrainIndexMetrics.IndexTag(Index);
        var tree = _fixture.Cluster.GrainFactory
            .GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(Index));

        for (var i = 0; i < _people.Length; i++)
        {
            var person = _people[i];
            var projection = projector.Project(person.Key, person.State);
            var plan = GrainIndexUpdatePlan.Between(
                GrainIndexProjection.Empty(person.Key), projection);

            using (LatticeSystemOrigin.Enter())
            {
                await GrainIndexPlanApplier.ApplyAsync(
                    tree, plan, indexTag, $"e2e-superseded-{person.Key}", CancellationToken.None);
            }

            await enrollments.CompleteAsync(Index, person.Key, projection, CancellationToken.None);
        }
    }

    private static List<GrainIndexEntry> LiveProjectionOf(EndToEndPerson[] people)
    {
        var projector = new GrainIndexProjector<IEndToEndDriftUserGrain, EndToEndUserState>(
            new GrainIndexDefinition<IEndToEndDriftUserGrain, EndToEndUserState>(
                Index,
                StringGrainKeyCodec<IEndToEndDriftUserGrain>.Instance,
                [
                    new TypedGrainIndexProperty<EndToEndUserState, int>("Age", static s => s.Age),
                    new TypedGrainIndexProperty<EndToEndUserState, string>("Country", static s => s.Country),
                ]));

        var expected = new List<GrainIndexEntry>(people.Length * EntriesPerGrain);
        for (var i = 0; i < people.Length; i++)
        {
            var entries = projector.Project(people[i].Key, people[i].State).Entries;
            for (var j = 0; j < entries.Count; j++)
                expected.Add(entries[j]);
        }

        return expected;
    }

    private static List<string> PropertyNamesOf(List<GrainIndexEntry> entries)
    {
        var names = new List<string>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            if (GrainIndexKeyEncoder.TryParseKey(entries[i].Key, out var name, out _, out _))
                names.Add(name);
        }

        return names;
    }

    private static string[] BritishKeys(EndToEndPerson[] people)
    {
        var matches = new List<string>(people.Length);
        for (var i = 0; i < people.Length; i++)
        {
            if (string.Equals(people[i].State.Country, "GB", StringComparison.Ordinal))
                matches.Add(people[i].Key);
        }

        return [.. matches];
    }

    [Test]
    public async Task The_superseded_declaration_really_did_build_an_index_of_the_old_shape()
    {
        var entries = await _fixture.IndexEntriesAsync(Index);
        var enrolled = await _fixture.EnrolledKeysAsync(Index);
        var names = PropertyNamesOf(entries);

        Assert.Multiple(() =>
        {
            Assert.That(entries, Has.Count.EqualTo(Population * EntriesPerGrain));
            Assert.That(names, Does.Contain("Nickname"),
                "The rebuild tests are worthless unless the index genuinely starts out holding "
                + "entries for a property the live declaration does not project.");
            Assert.That(names, Does.Not.Contain("Country"));
            Assert.That(enrolled, Has.Count.EqualTo(Population));
        });
    }

    [Test]
    public async Task A_breaking_declaration_change_under_the_reject_policy_fails_and_names_the_field()
    {
        Assert.That(
            async () => await ReconcileAsync(DeclareLiveShape, GrainIndexDriftPolicy.Reject),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.ChangedFields))
                .Contains(GrainIndexDefinitionField.Properties)
                .And.Message.Contains(Index));

        var record = await _fixture.SiloServices
            .GetRequiredService<IGrainIndexRegistryStore>()
            .ReadAsync(Index, CancellationToken.None);

        var entries = await _fixture.IndexEntriesAsync(Index);
        var stillQueryable = await _fixture.Index<IEndToEndDriftUserGrain>(Index)
            .Where(s => s.Age >= 30)
            .ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                record!.Descriptor.Properties.Select(p => p.Name),
                Is.EqualTo(new[] { "Age", "Nickname" }),
                "A rejected start must leave the stored declaration exactly as it was.");
            Assert.That(entries, Has.Count.EqualTo(Population * EntriesPerGrain),
                "Rejecting a change must not destroy the index the running cluster is still serving.");
            Assert.That(stillQueryable, Has.Count.EqualTo(4),
                "The surviving entries have to remain queryable, or a rejected rollout would take "
                + "the index down as surely as accepting a bad one.");
        });
    }

    [Test]
    public async Task The_rebuild_policy_accepts_the_change_and_schedules_the_crawl()
    {
        await ReconcileAsync(DeclareLiveShape, GrainIndexDriftPolicy.Rebuild);

        var record = await _fixture.SiloServices
            .GetRequiredService<IGrainIndexRegistryStore>()
            .ReadAsync(Index, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                record!.Descriptor.Properties.Select(p => p.Name),
                Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(record.NeedsBackfill, Is.True,
                "Accepting a breaking change without scheduling a rebuild would leave the index "
                + "permanently describing a declaration nobody is running.");
            Assert.That(
                record.Fingerprint,
                Is.EqualTo(GrainIndexFingerprint.Compute(record.Descriptor, record.KeyCodecId)));
        });
    }

    [Test]
    public async Task A_rebuild_revisits_every_enrolled_grain_and_converges_on_the_new_declaration()
    {
        await ReconcileAsync(DeclareLiveShape, GrainIndexDriftPolicy.Rebuild);

        var restarted = await _fixture.Admin.RebuildAsync(Index);
        await _fixture.DrainBackfillAsync(Index);

        var status = await _fixture.Admin.GetStatusAsync(Index);
        var entries = await _fixture.IndexEntriesAsync(Index);
        var names = PropertyNamesOf(entries);
        var british = await _fixture.Index<IEndToEndDriftUserGrain>(Index)
            .Where(s => s.Country == "GB")
            .ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(restarted.RevisitsEnrolled, Is.True,
                "A rebuild that skipped already-enrolled grains could never replace their entries.");
            Assert.That(status.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Backfill.Enrolled, Is.EqualTo(Population),
                "Every grain the superseded declaration enrolled has to be visited again.");

            Assert.That(names, Does.Not.Contain("Nickname"),
                "The superseded declaration's entries are orphaned by the change, so a converged "
                + "rebuild must have removed them.");
            Assert.That(entries, Is.EquivalentTo(LiveProjectionOf(_people)),
                "The rebuilt index must be exactly what the new declaration projects from the "
                + "grains it re-visited - no more and no less.");
            Assert.That(british, Is.EquivalentTo(BritishKeys(_people)),
                "A property the index did not previously hold has to be queryable once the "
                + "rebuild has run.");
            Assert.That(british, Is.Unique);
        });
    }
}
