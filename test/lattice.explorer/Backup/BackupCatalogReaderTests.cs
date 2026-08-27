using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

[TestFixture]
public class BackupCatalogReaderTests
{
    private static BackupCatalogReader CreateReader(FakeBackupControlClient client) => new(client);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new BackupCatalogReader(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task LoadPageAsync_success_returns_entries()
    {
        var client = new FakeBackupControlClient
        {
            ListResult = new BackupCatalogPage
            {
                Entries = new[] { SampleBackup.Manifest("b1") },
                NextPageToken = "next",
            },
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(view.Entries, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task LoadPageAsync_denied_returns_denied_view()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new LatticeAuthorizationDeniedException("no list for you"),
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.False);
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(view.Message, Is.EqualTo("no list for you"));
            Assert.That(view.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task LoadPageAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeBackupControlClient
        {
            ListThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };

        var view = await CreateReader(client).LoadPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(view.Message, Does.Contain("Unavailable"));
        });
    }

    [Test]
    public async Task TriggerFullAsync_success_returns_success_result()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).TriggerFullAsync("nightly", BackupScopeSelector.WholeTree("tree-a"));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(result.Message, Does.Contain("full-1"));
        });
    }

    [Test]
    public async Task TriggerFullAsync_denied_degrades_gracefully()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new LatticeAuthorizationDeniedException("capture denied"),
        };

        var result = await CreateReader(client).TriggerFullAsync("nightly", BackupScopeSelector.WholeTree("tree-a"));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("capture denied"));
        });
    }

    [Test]
    public async Task TriggerIncrementalAsync_success_returns_success_result()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).TriggerIncrementalAsync("delta", BackupScopeSelector.WholeTree("tree-a"), "base-1");

        Assert.That(result.Message, Does.Contain("inc-1"));
    }

    [Test]
    public async Task TriggerSetAsync_success_reports_set_id_and_member_count()
    {
        var client = new FakeBackupControlClient();
        var scopes = new[]
        {
            BackupScopeSelector.WholeTree("tree-a"),
            BackupScopeSelector.WholeTree("tree-b"),
        };

        var result = await CreateReader(client).TriggerSetAsync("nightly-set", scopes, crossTreeConsistent: true);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("set-1"));
            Assert.That(result.Message, Does.Contain("2"));
            Assert.That(client.LastSetRequest, Is.Not.Null);
            Assert.That(client.LastSetRequest!.Scopes, Has.Count.EqualTo(2));
            Assert.That(client.LastSetRequest.CrossTreeConsistent, Is.True);
        });
    }

    [Test]
    public async Task TriggerSetAsync_without_a_set_id_reports_the_set_name_instead_of_an_empty_id()
    {
        // A single-scope capture reports no set id. The summary must not render an
        // empty quoted id ("Captured backup set ''"); it names the set and says why
        // there is no id to group by.
        var client = new FakeBackupControlClient { SetCaptureId = null };
        var scopes = new[] { BackupScopeSelector.WholeTree("tree-a") };

        var result = await CreateReader(client).TriggerSetAsync("nightly-set", scopes, crossTreeConsistent: false);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Not.Contain("''"));
            Assert.That(result.Message, Does.Contain("nightly-set"));
            Assert.That(result.Message, Does.Contain("no set id"));
        });
    }

    [Test]
    public async Task TriggerSetAsync_denied_degrades_gracefully()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new LatticeAuthorizationDeniedException("set capture denied"),
        };
        var scopes = new[] { BackupScopeSelector.WholeTree("tree-a") };

        var result = await CreateReader(client).TriggerSetAsync("nightly-set", scopes, crossTreeConsistent: false);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("set capture denied"));
        });
    }

    [Test]
    public void TriggerSetAsync_empty_name_throws()
    {
        var client = new FakeBackupControlClient();

        Assert.That(
            () => CreateReader(client).TriggerSetAsync(string.Empty, new[] { BackupScopeSelector.WholeTree("t") }, false),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RestoreAsync_success_reports_target_and_entries()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("tree-b"));
            Assert.That(result.Message, Does.Contain("7"));
        });
    }

    [Test]
    public async Task RestoreAsync_defaults_to_in_place_mode()
    {
        var client = new FakeBackupControlClient();

        await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.That(client.LastRestoreRequest!.Mode, Is.EqualTo(LatticeRestoreMode.InPlace));
    }

    [Test]
    public async Task RestoreAsync_forwards_shadow_cutover_mode()
    {
        var client = new FakeBackupControlClient();

        await CreateReader(client).RestoreAsync("b1", "tree-b", LatticeRestoreMode.ShadowCutover);

        Assert.That(client.LastRestoreRequest!.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
    }

    [Test]
    public async Task RestoreAsync_transport_failure_returns_failed_result()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new RpcException(new Status(StatusCode.Internal, "boom")),
        };

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Failed));
    }

    [Test]
    public async Task RestoreAsync_unshared_sink_precondition_explains_the_misconfiguration()
    {
        // The coordinated-restore saga aborts because a peer cluster could not
        // prepare (its backup store is not the shared one). The server reports
        // FailedPrecondition; the reader must turn it into an actionable
        // "not shared across every cluster" explanation, not a raw status dump.
        var client = new FakeBackupControlClient
        {
            MutationThrows = new RpcException(new Status(
                StatusCode.FailedPrecondition,
                "Coordinated restore of backup 'b1' into replicated tree 'tree-b' aborted: "
                + "at least one cluster could not prepare. Every cluster was compensated back "
                + "to its pre-restore state.")),
        };

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(result.Message, Does.Contain("shared"));
            Assert.That(result.Message, Does.Contain("every cluster"));
        });
    }

    [Test]
    public async Task RestoreAsync_absent_artifact_precondition_explains_the_misconfiguration()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new RpcException(new Status(
                StatusCode.FailedPrecondition,
                "Backup 'b1' references artifact 'a1', which is absent from the sink.")),
        };

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(result.Message, Does.Contain("shared backup sink"));
        });
    }

    [Test]
    public async Task RestoreAsync_other_precondition_surfaces_the_server_detail()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new RpcException(new Status(
                StatusCode.FailedPrecondition,
                "Artifact 'a1' of backup 'b1' failed integrity validation.")),
        };

        var result = await CreateReader(client).RestoreAsync("b1", "tree-b");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(result.Message, Does.Contain("failed integrity validation"));
            Assert.That(result.Message, Does.Not.Contain("shared"));
        });
    }

    [Test]
    public async Task DeleteAsync_absent_reports_already_absent()
    {
        var client = new FakeBackupControlClient { DeleteResult = false };

        var result = await CreateReader(client).DeleteAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("already absent"));
        });
    }

    [Test]
    public void TriggerFullAsync_empty_name_throws()
    {
        var client = new FakeBackupControlClient();

        Assert.That(
            () => CreateReader(client).TriggerFullAsync(string.Empty, BackupScopeSelector.WholeTree("t")),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ScheduleAsync_success_reports_the_effective_cadence_and_forwards_the_request()
    {
        var client = new FakeBackupControlClient { ScheduleResult = TimeSpan.FromMinutes(90) };
        var scope = BackupScopeSelector.WholeTree("tree-a");

        var result = await CreateReader(client).ScheduleAsync(scope, incremental: false, TimeSpan.FromMinutes(90));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("full"));
            Assert.That(result.Message, Does.Contain("1h 30m"));
            Assert.That(client.LastScheduledScope, Is.EqualTo(scope));
            Assert.That(client.LastScheduledIncremental, Is.False);
            Assert.That(client.LastScheduledInterval, Is.EqualTo(TimeSpan.FromMinutes(90)));
        });
    }

    [Test]
    public async Task ScheduleAsync_incremental_reports_the_incremental_kind()
    {
        var client = new FakeBackupControlClient { ScheduleResult = TimeSpan.FromMinutes(15) };

        var result = await CreateReader(client)
            .ScheduleAsync(BackupScopeSelector.WholeTree("tree-a"), incremental: true, TimeSpan.FromMinutes(15));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Does.Contain("incremental"));
            Assert.That(result.Message, Does.Contain("15m"));
        });
    }

    [Test]
    public async Task ScheduleAsync_denied_degrades_gracefully()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new LatticeAuthorizationDeniedException("schedule denied"),
        };

        var result = await CreateReader(client)
            .ScheduleAsync(BackupScopeSelector.WholeTree("tree-a"), incremental: false, TimeSpan.FromMinutes(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("schedule denied"));
        });
    }

    [Test]
    public void ScheduleAsync_non_positive_interval_throws()
    {
        var client = new FakeBackupControlClient();

        Assert.That(
            () => CreateReader(client).ScheduleAsync(BackupScopeSelector.WholeTree("t"), incremental: false, TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task UnscheduleAsync_success_reports_the_removed_kind_and_forwards_the_request()
    {
        var client = new FakeBackupControlClient();
        var scope = BackupScopeSelector.WholeTree("tree-a");

        var result = await CreateReader(client).UnscheduleAsync(scope, incremental: true);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Message, Is.EqualTo("Removed the recurring incremental backup schedule."));
            Assert.That(client.LastCanceledScope, Is.EqualTo(scope));
            Assert.That(client.LastCanceledIncremental, Is.True);
        });
    }

    [Test]
    public async Task UnscheduleAsync_denied_degrades_gracefully()
    {
        var client = new FakeBackupControlClient
        {
            MutationThrows = new LatticeAuthorizationDeniedException("unschedule denied"),
        };

        var result = await CreateReader(client)
            .UnscheduleAsync(BackupScopeSelector.WholeTree("tree-a"), incremental: false);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(BackupOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("unschedule denied"));
        });
    }

    [Test]
    public async Task GetScheduleStatusAsync_returns_scope_status()
    {
        var scope = BackupScopeSelector.WholeTree("tree-a");
        var expected = new BackupScopeStatus(
            scope,
            fullScheduleRegistered: true,
            incrementalScheduleRegistered: false,
            lastFullRunUtc: null,
            lastFullSuccessUtc: null,
            lastIncrementalRunUtc: null,
            lastIncrementalSuccessUtc: null,
            lastRunOutcome: BackupScopeRunOutcome.None,
            chainDepth: 0,
            runtimeFullBackupInterval: TimeSpan.FromMinutes(20));
        var client = new FakeBackupControlClient { ScopeStatusResult = expected };

        var actual = await CreateReader(client).GetScheduleStatusAsync(scope);

        Assert.That(actual, Is.SameAs(expected));
    }

    [Test]
    public async Task GetScheduleStatusAsync_returns_null_for_denial_or_transport_failure()
    {
        var denied = new FakeBackupControlClient
        {
            ScopeStatusThrows = new LatticeAuthorizationDeniedException("status denied"),
        };
        var failed = new FakeBackupControlClient
        {
            ScopeStatusThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };

        var deniedStatus = await CreateReader(denied).GetScheduleStatusAsync(BackupScopeSelector.WholeTree("tree-a"));
        var failedStatus = await CreateReader(failed).GetScheduleStatusAsync(BackupScopeSelector.WholeTree("tree-a"));

        Assert.Multiple(() =>
        {
            Assert.That(deniedStatus, Is.Null);
            Assert.That(failedStatus, Is.Null);
        });
    }

    // ---- Health ---------------------------------------------------------

    [Test]
    public async Task IsHealthMonitoringAvailableAsync_reflects_client_result()
    {
        var client = new FakeBackupControlClient { HealthAvailableResult = true };

        Assert.That(await CreateReader(client).IsHealthMonitoringAvailableAsync(), Is.True);
    }

    [Test]
    public async Task IsHealthMonitoringAvailableAsync_returns_false_on_denial_or_transport_failure()
    {
        var denied = new FakeBackupControlClient
        {
            HealthThrows = new LatticeAuthorizationDeniedException("health denied"),
        };
        var failed = new FakeBackupControlClient
        {
            HealthThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };

        Assert.Multiple(async () =>
        {
            Assert.That(await CreateReader(denied).IsHealthMonitoringAvailableAsync(), Is.False);
            Assert.That(await CreateReader(failed).IsHealthMonitoringAvailableAsync(), Is.False);
        });
    }

    [Test]
    public async Task GetHealthAsync_returns_stored_report()
    {
        var report = new BackupHealthReport(
            "b1", BackupHealthStatus.Warning, manifestPresent: true,
            new[] { "art-1" }, Array.Empty<string>(), DateTimeOffset.UtcNow, "Missing artifact art-1.");
        var client = new FakeBackupControlClient { HealthReportResult = report };

        Assert.That(await CreateReader(client).GetHealthAsync("b1"), Is.SameAs(report));
    }

    [Test]
    public async Task GetHealthAsync_returns_null_on_denial_or_transport_failure()
    {
        var denied = new FakeBackupControlClient
        {
            HealthThrows = new LatticeAuthorizationDeniedException("health denied"),
        };

        Assert.That(await CreateReader(denied).GetHealthAsync("b1"), Is.Null);
    }

    [Test]
    public void GetHealthAsync_empty_id_throws()
    {
        Assert.That(
            () => CreateReader(new FakeBackupControlClient()).GetHealthAsync(string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public async Task CheckHealthAsync_success_reports_status_and_explanation()
    {
        var report = new BackupHealthReport(
            "b1", BackupHealthStatus.Healthy, manifestPresent: true,
            Array.Empty<string>(), Array.Empty<string>(), DateTimeOffset.UtcNow, "All good.");
        var client = new FakeBackupControlClient { HealthReportResult = report };

        var result = await CreateReader(client).CheckHealthAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastCheckedBackupId, Is.EqualTo("b1"));
            Assert.That(result.Message, Does.Contain("Healthy").And.Contain("All good."));
        });
    }

    [Test]
    public async Task ConfigureHealthAsync_success_forwards_config()
    {
        var client = new FakeBackupControlClient();

        var result = await CreateReader(client).ConfigureHealthAsync("b1", enabled: false, TimeSpan.FromHours(3));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastConfiguredBackupId, Is.EqualTo("b1"));
            Assert.That(client.LastHealthConfig!.MonitoringEnabled, Is.False);
            Assert.That(client.LastHealthConfig.Interval, Is.EqualTo(TimeSpan.FromHours(3)));
        });
    }

    [Test]
    public void ConfigureHealthAsync_non_positive_interval_throws()
    {
        Assert.That(
            () => CreateReader(new FakeBackupControlClient()).ConfigureHealthAsync("b1", true, TimeSpan.Zero),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
