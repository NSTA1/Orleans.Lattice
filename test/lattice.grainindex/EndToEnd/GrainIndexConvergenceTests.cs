namespace Orleans.Lattice.GrainIndex.Tests.EndToEnd;

/// <summary>
/// The whole package end to end: a real silo, a real registry tree, real index
/// trees, real grains, and the two routes a grain can be onboarded by. These
/// prove the property the package exists to have - that the index converges on
/// exactly the grains it should hold, by either route, and stays converged when
/// grains are mutated underneath a crawl in flight.
/// </summary>
/// <remarks>
/// <para>
/// The active path and the backfill path are given separate indexes over
/// separate grain types but the <b>same key strings and the same states</b>. An
/// entry key carries the property name, the encoded value, and the encoded grain
/// key, and a string-keyed grain's encoded key is its key verbatim, so a correct
/// implementation must produce byte-identical entry sets on the two routes.
/// Asserting set equality between them is therefore a direct comparison, not a
/// translation, and it fails on a duplicate or a missing grain just as loudly as
/// on a wrong value.
/// </para>
/// <para>
/// Nothing in this fixture waits. The crawl's reminder and timer are off and the
/// outbox drain is off, so every pass is invoked explicitly and the drain loop is
/// bounded by a pass count derived from the population size. There is no
/// <c>Task.Delay</c>, no timeout, no clock reading, and no dependence on a
/// deactivation having settled: each test seeds a population under fresh keys.
/// </para>
/// <para>
/// It is <see cref="NonParallelizableAttribute"/> because the key sources the
/// silo resolves are process-wide singletons reading static population state,
/// and because the indexes it resets are shared cluster state.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class GrainIndexConvergenceTests
{
    private const string ActiveIndex = GrainIndexEndToEndClusterFixture.ActiveIndex;
    private const string BackfillIndex = GrainIndexEndToEndClusterFixture.BackfillIndex;
    private const int Population = GrainIndexEndToEndClusterFixture.PopulationSize;
    private const int BatchSize = GrainIndexEndToEndClusterFixture.BatchSize;
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
        _people = GrainIndexEndToEndClusterFixture.Population($"conv{++_runId}");
        await _fixture.ResetIndexAsync(ActiveIndex);
        await _fixture.ResetIndexAsync(BackfillIndex);
    }

    /// <summary>
    /// The entry set the live declaration produces for <paramref name="people"/>,
    /// computed by the real projector so the expectation is derived from the
    /// declaration rather than restated by hand.
    /// </summary>
    private static List<GrainIndexEntry> ProjectionOf(EndToEndPerson[] people)
    {
        var projector = new GrainIndexProjector<IEndToEndDormantUserGrain, EndToEndUserState>(
            GrainIndexEndToEndClusterFixture.LiveDefinition());

        var expected = new List<GrainIndexEntry>(people.Length * EntriesPerGrain);
        for (var i = 0; i < people.Length; i++)
        {
            var entries = projector.Project(people[i].Key, people[i].State).Entries;
            for (var j = 0; j < entries.Count; j++)
                expected.Add(entries[j]);
        }

        return expected;
    }

    private static string[] KeysWhere(EndToEndPerson[] people, Func<EndToEndUserState, bool> predicate)
    {
        var matches = new List<string>(people.Length);
        for (var i = 0; i < people.Length; i++)
        {
            if (predicate(people[i].State))
                matches.Add(people[i].Key);
        }

        return [.. matches];
    }

    private static string[] AllKeys(EndToEndPerson[] people)
    {
        var keys = new string[people.Length];
        for (var i = 0; i < people.Length; i++)
            keys[i] = people[i].Key;

        return keys;
    }

    [Test]
    public async Task Grains_written_while_the_index_is_live_answer_queries_with_exactly_the_matching_grains()
    {
        await _fixture.WriteThroughGrainsAsync(_people);
        var index = _fixture.Index<IEndToEndActiveUserGrain>(ActiveIndex);

        var adults = await index.Where(s => s.Age >= 30).ToKeyListAsync();
        var british = await index.Where(s => s.Country == "GB").ToKeyListAsync();
        var narrow = await index.Where(s => s.Age >= 30 && s.Country == "GB").ToKeyListAsync();
        var none = await index.Where(s => s.Country == "ZZ").ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(adults, Is.EquivalentTo(KeysWhere(_people, static s => s.Age >= 30)));
            Assert.That(british, Is.EquivalentTo(KeysWhere(_people, static s => s.Country == "GB")));
            Assert.That(
                narrow,
                Is.EquivalentTo(KeysWhere(_people, static s => s.Age >= 30 && s.Country == "GB")),
                "A conjunction across two properties is a semi-join over two scans, so it is the "
                + "shape most likely to lose or duplicate a grain.");
            Assert.That(none, Is.Empty);

            Assert.That(adults, Is.Unique, "One grain contributes one entry per property, never two.");
            Assert.That(british, Is.Unique);
        });
    }

    [Test]
    public async Task The_activation_path_writes_exactly_one_entry_per_property_per_grain()
    {
        await _fixture.WriteThroughGrainsAsync(_people);

        var entries = await _fixture.IndexEntriesAsync(ActiveIndex);
        var enrolled = await _fixture.EnrolledKeysAsync(ActiveIndex);

        Assert.Multiple(() =>
        {
            Assert.That(entries, Is.EquivalentTo(ProjectionOf(_people)));
            Assert.That(entries, Has.Count.EqualTo(Population * EntriesPerGrain));
            Assert.That(enrolled, Is.EquivalentTo(AllKeys(_people)));
        });
    }

    [Test]
    public async Task A_population_that_predates_the_index_is_onboarded_entirely_by_the_backfill_crawl()
    {
        await _fixture.SeedDormantBackfillAsync(_people);

        var beforeCrawl = await _fixture.IndexEntriesAsync(BackfillIndex);

        await _fixture.Admin.RebuildAsync(BackfillIndex);
        await _fixture.DrainBackfillAsync(BackfillIndex);

        var afterCrawl = await _fixture.IndexEntriesAsync(BackfillIndex);
        var status = await _fixture.Admin.GetStatusAsync(BackfillIndex);
        var index = _fixture.Index<IEndToEndDormantUserGrain>(BackfillIndex);
        var british = await index.Where(s => s.Country == "GB").ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(beforeCrawl, Is.Empty,
                "The population was seeded straight into storage, so nothing can have indexed it yet.");

            Assert.That(status.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Backfill.Enrolled, Is.EqualTo(Population));
            Assert.That(afterCrawl, Is.EquivalentTo(ProjectionOf(_people)));
            Assert.That(british, Is.EquivalentTo(KeysWhere(_people, static s => s.Country == "GB")));
            Assert.That(british, Is.Unique);
        });
    }

    [Test]
    public async Task The_activation_path_and_the_backfill_path_produce_an_identical_index()
    {
        await _fixture.WriteThroughGrainsAsync(_people);

        await _fixture.SeedDormantBackfillAsync(_people);
        await _fixture.Admin.RebuildAsync(BackfillIndex);
        await _fixture.DrainBackfillAsync(BackfillIndex);

        var byActivation = await _fixture.IndexEntriesAsync(ActiveIndex);
        var byBackfill = await _fixture.IndexEntriesAsync(BackfillIndex);

        var activationKeys = await _fixture.Index<IEndToEndActiveUserGrain>(ActiveIndex)
            .Where(s => s.Age >= 30)
            .ToKeyListAsync();
        var backfillKeys = await _fixture.Index<IEndToEndDormantUserGrain>(BackfillIndex)
            .Where(s => s.Age >= 30)
            .ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(byBackfill, Is.EquivalentTo(byActivation),
                "The two onboarding routes have to converge on the same entries for the same "
                + "population, or which route a grain arrived by would be observable in a query.");
            Assert.That(byActivation, Is.EquivalentTo(ProjectionOf(_people)));
            Assert.That(byActivation, Has.Count.EqualTo(Population * EntriesPerGrain),
                "Two routes onto the same grain must not file the entry twice.");

            Assert.That(backfillKeys, Is.EquivalentTo(activationKeys));
            Assert.That(activationKeys, Is.EquivalentTo(KeysWhere(_people, static s => s.Age >= 30)));
        });
    }

    [Test]
    public async Task Mutations_landing_while_a_crawl_is_in_flight_converge_on_the_final_grain_states()
    {
        await _fixture.SeedDormantBackfillAsync(_people);
        var started = await _fixture.Admin.RebuildAsync(BackfillIndex);

        // The interleaving is chosen rather than raced. A mutation is applied
        // after a pass has crawled some of the population and before it has
        // crawled the rest, which is exactly the state a real concurrent write
        // finds the crawl in - but it is reached by stepping the crawl, so the
        // assertion below does not depend on which of two threads won.
        var afterFirstPass = await _fixture.Admin.RunBackfillPassAsync(BackfillIndex);

        // One grain the crawl has already visited and one it has not, mutated
        // together, so the test covers both an update to an entry the crawl
        // wrote and an activation-path enrolment that beats the crawl to a grain.
        var crawled = _people[0];
        var notYetCrawled = _people[Population - 1];
        await Task.WhenAll(
            MutateAsync(crawled.Key, 91, "NZ"),
            MutateAsync(notYetCrawled.Key, 92, "NZ"));

        await _fixture.Admin.RunBackfillPassAsync(BackfillIndex);

        var middle = _people[Population - 2];
        await MutateAsync(middle.Key, 93, "NZ");

        await _fixture.DrainBackfillAsync(BackfillIndex);

        var expected = FinalStates(
            (crawled.Key, 91, "NZ"),
            (notYetCrawled.Key, 92, "NZ"),
            (middle.Key, 93, "NZ"));

        var entries = await _fixture.IndexEntriesAsync(BackfillIndex);
        var enrolled = await _fixture.EnrolledKeysAsync(BackfillIndex);
        var status = await _fixture.Admin.GetStatusAsync(BackfillIndex);

        var index = _fixture.Index<IEndToEndDormantUserGrain>(BackfillIndex);
        var moved = await index.Where(s => s.Country == "NZ").ToKeyListAsync();
        var untouched = await index.Where(s => s.Country == "GB").ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(started.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(afterFirstPass.Visited, Is.EqualTo(BatchSize),
                "The mutation has to land against a partially crawled population, or the test is "
                + "not exercising churn at all.");

            Assert.That(status.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(entries, Is.EquivalentTo(ProjectionOf(expected)),
                "Once the crawl has finished and the mutations have settled, the index must be "
                + "exactly a fresh projection of the final grain states - no stale entry from a "
                + "value a grain has moved off, and no entry missing for one it moved onto.");
            Assert.That(entries, Has.Count.EqualTo(Population * EntriesPerGrain));
            Assert.That(enrolled, Has.Count.EqualTo(Population),
                "Every grain is enrolled exactly once however many routes reached it.");

            Assert.That(moved, Is.EquivalentTo(KeysWhere(expected, static s => s.Country == "NZ")));
            Assert.That(untouched, Is.EquivalentTo(KeysWhere(expected, static s => s.Country == "GB")));
            Assert.That(moved, Is.Unique);
        });
    }

    private Task MutateAsync(string key, int age, string country) =>
        _fixture.Cluster.GrainFactory.GetGrain<IEndToEndDormantUserGrain>(key).SetAsync(age, country);

    /// <summary>
    /// The population as it stands after the listed mutations, so the expected
    /// projection is derived from the same states the grains ended up holding.
    /// </summary>
    private EndToEndPerson[] FinalStates(params (string Key, int Age, string Country)[] mutations)
    {
        var final = new EndToEndPerson[_people.Length];
        for (var i = 0; i < _people.Length; i++)
        {
            var person = _people[i];
            var age = person.State.Age;
            var country = person.State.Country;

            for (var m = 0; m < mutations.Length; m++)
            {
                if (!string.Equals(mutations[m].Key, person.Key, StringComparison.Ordinal))
                    continue;

                age = mutations[m].Age;
                country = mutations[m].Country;
            }

            final[i] = new EndToEndPerson(
                person.Key,
                new EndToEndUserState
                {
                    Age = age,
                    Country = country,
                    Nickname = person.State.Nickname,
                });
        }

        return final;
    }
}
