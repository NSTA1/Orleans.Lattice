using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Observability;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Lattice.GrainIndex.Tests.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// Covers <c>GrainIndexAdmin</c> as a unit: the declaration lookup, the status
/// assembly from the registry and the crawl, the population bound, and the fact
/// that every control delegates to the index's own backfill primitive rather
/// than reimplementing it.
/// </summary>
[TestFixture]
public sealed class GrainIndexAdminTests
{
    private const string Index = "users";
    private const string Other = "orders";

    [Test]
    public void The_admin_rejects_a_null_declaration_source() =>
        Assert.That(
            () => new GrainIndexAdmin(
                null!,
                Substitute.For<IOptionsMonitor<GrainIndexOptions>>(),
                new FakeGrainIndexRegistryStore(),
                Substitute.For<IGrainKeySourceResolver>(),
                Substitute.For<IGrainFactory>()),
            Throws.ArgumentNullException);

    [Test]
    public void The_admin_rejects_a_null_options_monitor() =>
        Assert.That(
            () => new GrainIndexAdmin(
                Declarations(),
                null!,
                new FakeGrainIndexRegistryStore(),
                Substitute.For<IGrainKeySourceResolver>(),
                Substitute.For<IGrainFactory>()),
            Throws.ArgumentNullException);

    [Test]
    public void The_admin_rejects_a_null_registry_store() =>
        Assert.That(
            () => new GrainIndexAdmin(
                Declarations(),
                Substitute.For<IOptionsMonitor<GrainIndexOptions>>(),
                null!,
                Substitute.For<IGrainKeySourceResolver>(),
                Substitute.For<IGrainFactory>()),
            Throws.ArgumentNullException);

    [Test]
    public void The_admin_rejects_a_null_key_source_resolver() =>
        Assert.That(
            () => new GrainIndexAdmin(
                Declarations(),
                Substitute.For<IOptionsMonitor<GrainIndexOptions>>(),
                new FakeGrainIndexRegistryStore(),
                null!,
                Substitute.For<IGrainFactory>()),
            Throws.ArgumentNullException);

    [Test]
    public void The_admin_rejects_a_null_grain_factory() =>
        Assert.That(
            () => new GrainIndexAdmin(
                Declarations(),
                Substitute.For<IOptionsMonitor<GrainIndexOptions>>(),
                new FakeGrainIndexRegistryStore(),
                Substitute.For<IGrainKeySourceResolver>(),
                null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_declared_indexes_are_listed_in_declaration_order()
    {
        var harness = new AdminHarness(Index, Other);

        Assert.That(harness.Admin.DeclaredIndexes, Is.EqualTo(new[] { Index, Other }));
    }

    [Test]
    public void A_silo_declaring_nothing_lists_nothing() =>
        Assert.That(new AdminHarness().Admin.DeclaredIndexes, Is.Empty);

    [Test]
    public void Getting_a_status_rejects_a_null_index_name()
    {
        var harness = new AdminHarness(Index);

        Assert.That(
            () => harness.Admin.GetStatusAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Getting_a_status_for_an_undeclared_index_names_the_ones_that_are()
    {
        var harness = new AdminHarness(Index);

        Assert.That(
            () => harness.Admin.GetStatusAsync("nope"),
            Throws.TypeOf<GrainIndexNotDeclaredException>()
                .With.Property(nameof(GrainIndexNotDeclaredException.IndexName)).EqualTo("nope")
                .And.Property(nameof(GrainIndexNotDeclaredException.DeclaredIndexes)).EqualTo(new[] { Index }));
    }

    [Test]
    public async Task An_unregistered_index_reports_its_live_declaration_and_no_drift()
    {
        var harness = new AdminHarness(Index);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo(Index));
            Assert.That(status.Registered, Is.False);
            Assert.That(status.NeedsBackfill, Is.False);
            Assert.That(status.Definition.Name, Is.EqualTo(Index));
            Assert.That(status.Definition.TreeName, Is.EqualTo(GrainIndexTreeNames.ForIndex(Index)));
            Assert.That(status.Drift.HasDrift, Is.False);
            Assert.That(status.Fingerprint, Is.EqualTo(default(GrainIndexFingerprint)));
        });
    }

    [Test]
    public async Task A_registered_index_reports_the_stored_record()
    {
        var harness = new AdminHarness(Index);
        var record = harness.SeedRecord(Index, needsBackfill: true);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Registered, Is.True);
            Assert.That(status.NeedsBackfill, Is.True);
            Assert.That(status.Fingerprint, Is.EqualTo(record.Fingerprint));
            Assert.That(status.KeyCodecId, Is.EqualTo(record.KeyCodecId));
            Assert.That(status.Drift.HasDrift, Is.False);
        });
    }

    [Test]
    public async Task A_declaration_that_has_moved_reports_the_fields_that_drifted()
    {
        var harness = new AdminHarness(Index);
        harness.SeedRecord(
            Index,
            needsBackfill: false,
            descriptor: DescriptorFactory.Create(
                Index,
                properties: [new GrainIndexPropertyDescriptor("Age", "System.Int32")]));

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Drift.HasDrift, Is.True);
            Assert.That(status.Drift.HasBreakingChange, Is.True);
            Assert.That(status.Drift.ChangedFields, Does.Contain(GrainIndexDefinitionField.Properties));
        });
    }

    [Test]
    public async Task The_entry_count_comes_from_the_index_tree()
    {
        var harness = new AdminHarness(Index);
        harness.Tree.CountAsync(Arg.Any<CancellationToken>()).Returns(17);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.That(status.EntryCount, Is.EqualTo(17));
    }

    [Test]
    public async Task Progress_mirrors_the_crawls_position_and_last_error()
    {
        var harness = new AdminHarness(Index);
        harness.Status = new GrainIndexBackfillStatus(
            Index,
            GrainIndexBackfillState.Failed,
            resumeAfterKey: "user-9",
            visited: 9,
            enrolled: 8,
            skipped: 1,
            failed: 0,
            passes: 3,
            revisitsEnrolled: false,
            startedUtc: null,
            updatedUtc: null,
            completedUtc: null,
            failureMessage: "registry unavailable");

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Failed));
            Assert.That(status.Progress.Processed, Is.EqualTo(9));
            Assert.That(status.Progress.LastProcessedKey, Is.EqualTo("user-9"));
            Assert.That(status.Progress.LastError, Is.EqualTo("registry unavailable"));
        });
    }

    [Test]
    public async Task A_key_source_that_bounds_its_population_yields_a_percentage()
    {
        var harness = new AdminHarness(Index);
        harness.KeySource = new BoundedKeySource(50);
        harness.Status = RunningStatus(visited: 20);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Progress.Total, Is.EqualTo(50));
            Assert.That(status.Progress.PercentComplete, Is.EqualTo(40d));
        });
    }

    [Test]
    public async Task No_key_source_leaves_the_progress_a_processed_count()
    {
        var harness = new AdminHarness(Index);
        harness.Status = RunningStatus(visited: 20);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Progress.Processed, Is.EqualTo(20));
            Assert.That(status.Progress.Total, Is.Null);
            Assert.That(status.Progress.PercentComplete, Is.Null);
        });
    }

    [Test]
    public async Task A_key_source_that_cannot_bound_itself_leaves_the_progress_a_processed_count()
    {
        var harness = new AdminHarness(Index);
        harness.KeySource = new BoundedKeySource(null);
        harness.Status = RunningStatus(visited: 20);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.That(status.Progress.Total, Is.Null);
    }

    [Test]
    public async Task A_key_source_whose_estimate_throws_does_not_fail_the_status()
    {
        var harness = new AdminHarness(Index);
        harness.KeySource = new ThrowingKeySource();
        harness.Status = RunningStatus(visited: 4);

        var status = await harness.Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.Progress.Processed, Is.EqualTo(4));
            Assert.That(status.Progress.Total, Is.Null);
        });
    }

    [Test]
    public async Task Listing_statuses_returns_one_per_declared_index_in_order()
    {
        var harness = new AdminHarness(Index, Other);

        var statuses = await harness.Admin.ListStatusAsync();

        Assert.That(statuses.Select(s => s.IndexName), Is.EqualTo(new[] { Index, Other }));
    }

    [Test]
    public async Task Listing_statuses_on_a_silo_declaring_nothing_returns_nothing() =>
        Assert.That(await new AdminHarness().Admin.ListStatusAsync(), Is.Empty);

    [Test]
    public async Task Pausing_delegates_to_the_crawls_own_pause()
    {
        var harness = new AdminHarness(Index);

        await harness.Admin.PauseBackfillAsync(Index);

        await harness.Backfill.Received(1).PauseAsync();
    }

    [Test]
    public async Task Resuming_delegates_to_the_crawls_own_resume()
    {
        var harness = new AdminHarness(Index);

        await harness.Admin.ResumeBackfillAsync(Index);

        await harness.Backfill.Received(1).ResumeAsync();
    }

    [Test]
    public async Task Rebuilding_delegates_to_the_crawls_restart_rather_than_reimplementing_it()
    {
        var harness = new AdminHarness(Index);

        await harness.Admin.RebuildAsync(Index);

        await harness.Backfill.Received(1).RestartAsync();
    }

    [Test]
    public async Task Running_a_pass_delegates_to_the_crawls_own_batch()
    {
        var harness = new AdminHarness(Index);

        await harness.Admin.RunBackfillPassAsync(Index);

        await harness.Backfill.Received(1).RunBatchAsync();
    }

    [TestCase("pause")]
    [TestCase("resume")]
    [TestCase("rebuild")]
    [TestCase("pass")]
    public void Every_control_rejects_a_null_index_name(string control)
    {
        var harness = new AdminHarness(Index);

        Assert.That(() => Invoke(harness, control, null!), Throws.ArgumentNullException);
    }

    [TestCase("pause")]
    [TestCase("resume")]
    [TestCase("rebuild")]
    [TestCase("pass")]
    public void Every_control_rejects_an_undeclared_index(string control)
    {
        var harness = new AdminHarness(Index);

        Assert.That(() => Invoke(harness, control, "nope"), Throws.TypeOf<GrainIndexNotDeclaredException>());
    }

    [TestCase("pause")]
    [TestCase("resume")]
    [TestCase("rebuild")]
    [TestCase("pass")]
    public void Every_control_honours_an_already_cancelled_token(string control)
    {
        var harness = new AdminHarness(Index);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            () => Invoke(harness, control, Index, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Listing_statuses_honours_an_already_cancelled_token()
    {
        var harness = new AdminHarness(Index);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            async () => await harness.Admin.ListStatusAsync(cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private static Task Invoke(
        AdminHarness harness,
        string control,
        string indexName,
        CancellationToken cancellationToken = default) =>
        control switch
        {
            "pause" => harness.Admin.PauseBackfillAsync(indexName, cancellationToken),
            "resume" => harness.Admin.ResumeBackfillAsync(indexName, cancellationToken),
            "rebuild" => harness.Admin.RebuildAsync(indexName, cancellationToken),
            _ => harness.Admin.RunBackfillPassAsync(indexName, cancellationToken),
        };

    private static GrainIndexBackfillStatus RunningStatus(long visited) =>
        new(
            Index,
            GrainIndexBackfillState.Running,
            resumeAfterKey: null,
            visited,
            enrolled: visited,
            skipped: 0,
            failed: 0,
            passes: 1,
            revisitsEnrolled: false,
            startedUtc: null,
            updatedUtc: null,
            completedUtc: null,
            failureMessage: null);

    private static IOptions<GrainIndexDeclarationOptions> Declarations() =>
        Options.Create(new GrainIndexDeclarationOptions());

    /// <summary>An <see cref="IGrainKeySource"/> that reports a fixed bound.</summary>
    private sealed class BoundedKeySource(long? count) : IGrainKeySource
    {
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(count);
    }

    /// <summary>An <see cref="IGrainKeySource"/> whose bound estimate throws.</summary>
    private sealed class ThrowingKeySource : IGrainKeySource
    {
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
            throw new InvalidOperationException("the roster is unavailable");
    }
}
