namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The multi-clause AND path, where a conjunction over more than one projected
/// property becomes one scan per property whose grain keys are intersected.
/// <para>
/// The intersect passes probe the buffered candidate set through a span over the
/// scanned entry key rather than materialising a grain key per entry, and prune
/// the set in place rather than rebuilding it. Both are invisible from outside,
/// so the observable contract - which grains survive, which payload they carry,
/// and that every cursor is closed - is pinned here directly.
/// </para>
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public async Task Intersect_over_two_properties_keeps_only_the_grains_matching_both()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Intersect_over_three_properties_applies_every_pass()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && s.Country == "GB" && s.Status == TestStatus.Active));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Intersect_whose_later_clause_excludes_everything_yields_nothing()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "ZZ"));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Empty);
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task Intersect_whose_later_clause_excludes_nothing_keeps_every_candidate()
    {
        var index = Populated();

        // Every indexed grain has a country in the scanned range, so the second
        // pass keeps the whole driving set and the prune has nothing to drop.
        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country != "ZZ"));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Intersect_drops_a_candidate_that_survives_the_first_pass_but_not_the_second()
    {
        var index = Populated();

        // bob and carol both clear the age bound; only carol is in GB, and only
        // carol is Active, so each pass has a survivor and a casualty.
        var afterOne = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Status == TestStatus.Active));
        var afterTwo = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && s.Status == TestStatus.Active && s.Country == "DE"));

        Assert.Multiple(() =>
        {
            Assert.That(afterOne, Is.EquivalentTo(new[] { "carol" }));
            Assert.That(afterTwo, Is.Empty);
        });
    }

    [Test]
    public async Task Intersect_reports_the_driving_clause_payload()
    {
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index
            .Where(s => s.Country == "GB" && s.Age >= 18)
            .ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matches, Has.Count.EqualTo(1));
            Assert.That(matches[0].GrainKey, Is.EqualTo("carol"));

            // The driving clause is the most selective one, and it is the only
            // clause whose payload is ever transferred.
            Assert.That(matches[0].PropertyName, Is.EqualTo("Country"));
            Assert.That(matches[0].Value, Is.Not.Empty);
            Assert.That(
                System.Text.Encoding.UTF8.GetString(matches[0].Value),
                Does.Contain("\"Country\":\"GB\""));
        });
    }

    [Test]
    public async Task Intersect_does_not_confuse_a_grain_key_that_suffixes_another()
    {
        // The intersect pass locates the grain key as a span inside the scanned
        // entry key. "bob" is a suffix of "xbob", so a comparison that started
        // anywhere but immediately after the second separator would match both.
        var index = QueryTestIndex.Create(
            ("bob", QueryTestIndex.State(age: 20, country: "GB")),
            ("xbob", QueryTestIndex.State(age: 20, country: "FR")));

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob" }));
    }

    [Test]
    public async Task Intersect_matches_a_grain_key_containing_the_separator_free_encoding()
    {
        var index = QueryTestIndex.Create(
            ("a", QueryTestIndex.State(age: 20, country: "GB")),
            ("ab", QueryTestIndex.State(age: 20, country: "GB")),
            ("abc", QueryTestIndex.State(age: 20, country: "FR")));

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "a", "ab" }));
    }

    [Test]
    public async Task Intersect_under_stream_execution_opens_no_cursor()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && s.Country == "GB")
            .WithExecution(GrainIndexQueryExecution.Stream));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
            Assert.That(index.Tree.CursorsOpened, Is.Zero);
        });
    }

    [Test]
    public async Task Intersect_under_a_snapshot_cursor_closes_every_cursor_it_opened()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && s.Country == "GB")
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
            Assert.That(index.Tree.CursorsOpened, Is.EqualTo(2));
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task Intersect_pages_a_small_page_size_without_losing_a_candidate()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 0 && s.Country != "ZZ")
            .WithPageSize(1));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Union_of_two_intersections_reports_a_shared_grain_once()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => (s.Age >= 18 && s.Country == "GB") || (s.Age >= 18 && s.Status == TestStatus.Active)));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
            Assert.That(keys, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Intersect_yields_each_surviving_grain_exactly_once()
    {
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 20, country: "GB")),
            ("bob", QueryTestIndex.State(age: 21, country: "GB")),
            ("carol", QueryTestIndex.State(age: 22, country: "GB")),
            ("dave", QueryTestIndex.State(age: 23, country: "FR")));

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 20 && s.Country == "GB"));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol" }));
            Assert.That(keys.Distinct(), Has.Exactly(3).Items);
        });
    }
}
