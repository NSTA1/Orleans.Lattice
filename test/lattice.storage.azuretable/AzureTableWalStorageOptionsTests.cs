using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for <see cref="AzureTableWalStorageOptions"/> covering
/// every <see cref="AzureTableWalStorageOptions.Validate"/> failure
/// mode and the happy paths for each of the four supported
/// authentication shapes (connection string, <c>ServiceUri</c> +
/// token credential, <c>ServiceUri</c> + shared-key credential, and
/// a pre-built <c>TableServiceClient</c>).
/// </summary>
[TestFixture]
public class AzureTableWalStorageOptionsTests
{
    [Test]
    public void Validate_throws_when_TableName_is_empty()
    {
        var options = new AzureTableWalStorageOptions
        {
            TableName = "",
            ConnectionString = "UseDevelopmentStorage=true",
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_no_authentication_mode_is_configured()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ConnectionString_and_ServiceUri_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            ServiceUri = new Uri("https://example.table.core.windows.net"),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ConnectionString_and_TokenCredential_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TokenCredential = new FakeTokenCredential(),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ConnectionString_and_SharedKeyCredential_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ServiceUri_has_no_credential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_both_TokenCredential_and_SharedKeyCredential_are_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            TokenCredential = new FakeTokenCredential(),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_for_ConnectionString_mode()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_succeeds_for_ServiceUri_plus_TokenCredential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            TokenCredential = new FakeTokenCredential(),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_succeeds_for_ServiceUri_plus_SharedKeyCredential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void DefaultTableName_is_OrleansLatticeWal()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.TableName, Is.EqualTo(AzureTableWalStorageOptions.DefaultTableName));
        Assert.That(options.TableName, Is.EqualTo("OrleansLatticeWal"));
    }

    [Test]
    public void Validate_succeeds_for_prebuilt_ServiceClient_mode()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential()),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_throws_when_ServiceClient_and_ConnectionString_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential()),
            ConnectionString = "UseDevelopmentStorage=true",
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ServiceClient_and_ServiceUri_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential()),
            ServiceUri = new Uri("https://example.table.core.windows.net"),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ServiceClient_and_TokenCredential_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential()),
            TokenCredential = new FakeTokenCredential(),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ServiceClient_and_SharedKeyCredential_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential()),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void BuildServiceClient_returns_the_supplied_instance_verbatim_when_ServiceClient_is_set()
    {
        // Host-supplied client must be returned by reference - the
        // provider does not wrap or rebuild it. Hosts that share one
        // TableServiceClient across multiple Orleans components rely
        // on the provider routing through that exact instance.
        var configured = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential());
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = configured,
        };

        var built = options.BuildServiceClient();

        Assert.That(built, Is.SameAs(configured));
    }

    [Test]
    public void BuildServiceClient_ignores_ConfigureClientOptions_when_ServiceClient_is_set()
    {
        // When the host supplies a pre-built client, it owns the
        // TableClientOptions; the provider must not invoke any
        // ConfigureClientOptions delegate because the underlying client
        // is already constructed and the delegate has nowhere to apply.
        var configured = new TableServiceClient(new Uri("https://example.table.core.windows.net"), new FakeTokenCredential());
        var delegateInvoked = false;
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = configured,
            ConfigureClientOptions = _ => delegateInvoked = true,
        };

        _ = options.BuildServiceClient();

        Assert.That(delegateInvoked, Is.False);
    }

    [Test]
    public void BuildServiceClient_invokes_ConfigureClientOptions_for_ConnectionString_mode()
    {
        // Sanity-pin the legacy path: callers still relying on
        // ConnectionString mode continue to receive ConfigureClientOptions
        // callbacks unchanged.
        var delegateInvoked = false;
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            ConfigureClientOptions = _ => delegateInvoked = true,
        };

        _ = options.BuildServiceClient();

        Assert.That(delegateInvoked, Is.True);
    }

    [Test]
    public void BuildServiceClient_invokes_ConfigureClientOptions_for_TokenCredential_mode()
    {
        // Symmetry pin: the token-credential construction branch must
        // also surface the host's ConfigureClientOptions delegate.
        var delegateInvoked = false;
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            TokenCredential = new FakeTokenCredential(),
            ConfigureClientOptions = _ => delegateInvoked = true,
        };

        var built = options.BuildServiceClient();

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.Not.Null);
            Assert.That(delegateInvoked, Is.True);
        });
    }

    [Test]
    public void BuildServiceClient_invokes_ConfigureClientOptions_for_SharedKeyCredential_mode()
    {
        // Symmetry pin: the shared-key construction branch must also
        // surface the host's ConfigureClientOptions delegate.
        var delegateInvoked = false;
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
            ConfigureClientOptions = _ => delegateInvoked = true,
        };

        var built = options.BuildServiceClient();

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.Not.Null);
            Assert.That(delegateInvoked, Is.True);
        });
    }

    [Test]
    public void ServiceClient_default_is_null()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.ServiceClient, Is.Null);
    }

    [Test]
    public void EliminateCandidateRowOnHotPath_defaults_to_true()
    {
        // Pin the default to the throughput-campaign operating point.
        // Eliding the phase-0 C-row removes a server-side-serialised
        // round-trip on the shared per-shard manifest partition from
        // every batch's hot path; an A/B against real Azure Tables at
        // the 25k-writer saturation rung moved the sustained-ingest
        // watermark ~24x (from ~58k to ~1.38M entries). Reconciliation
        // falls back to the cross-partition discovery scan, and the
        // legacy C-row scan still runs first so pre-upgrade orphans
        // remain recoverable.
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.EliminateCandidateRowOnHotPath, Is.True);
    }

    [Test]
    public void EliminateCandidateRowOnHotPath_default_constant_is_true()
    {
        Assert.That(AzureTableWalStorageOptions.DefaultEliminateCandidateRowOnHotPath, Is.True);
    }

    [Test]
    public void EliminateCandidateRowOnHotPath_round_trips_when_set_to_false()
    {
        var options = new AzureTableWalStorageOptions
        {
            EliminateCandidateRowOnHotPath = false,
        };

        Assert.That(options.EliminateCandidateRowOnHotPath, Is.False);
    }

    [Test]
    public void Validate_succeeds_when_EliminateCandidateRowOnHotPath_is_set()
    {
        // The flag is orthogonal to the auth-mode + table-name
        // validation; flipping it on with a valid base configuration
        // must not introduce a new failure mode.
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            EliminateCandidateRowOnHotPath = true,
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    // ---------------------------------------------------------------
    // C4 retry-budget tuning knobs
    // ---------------------------------------------------------------
    //
    // The five new options (RetryMaxAttempts, RetryDelay, RetryMaxDelay,
    // RetryNetworkTimeout, RetryMode) are intentionally additive: null
    // on each leaves the Azure.Data.Tables SDK default in place, so
    // existing deployments see no behaviour change. When set, they are
    // applied to TableClientOptions.Retry BEFORE the host's
    // ConfigureClientOptions callback so the host wins any conflict.
    // The tests below exercise both halves of that ordering by reading
    // the resulting TableClientOptions inside a probe callback.

    [Test]
    public void RetryKnobs_default_to_null()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RetryMaxAttempts, Is.Null);
            Assert.That(options.RetryDelay, Is.Null);
            Assert.That(options.RetryMaxDelay, Is.Null);
            Assert.That(options.RetryNetworkTimeout, Is.Null);
            Assert.That(options.RetryMode, Is.Null);
        });
    }

    [Test]
    public void Validate_throws_when_RetryMaxAttempts_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMaxAttempts = -1,
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_RetryDelay_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryDelay = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_RetryMaxDelay_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMaxDelay = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_RetryDelay_exceeds_RetryMaxDelay()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryDelay = TimeSpan.FromSeconds(5),
            RetryMaxDelay = TimeSpan.FromSeconds(1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_RetryNetworkTimeout_is_zero()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryNetworkTimeout = TimeSpan.Zero,
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_with_all_retry_knobs_set_and_internally_consistent()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMaxAttempts = 2,
            RetryDelay = TimeSpan.FromMilliseconds(50),
            RetryMaxDelay = TimeSpan.FromMilliseconds(500),
            RetryNetworkTimeout = TimeSpan.FromSeconds(5),
            RetryMode = Azure.Core.RetryMode.Exponential,
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void BuildServiceClient_applies_RetryMaxAttempts_when_set()
    {
        TableClientOptions? observed = null;
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMaxAttempts = 1,
            ConfigureClientOptions = co => observed = co,
        };

        _ = options.BuildServiceClient();

        Assert.That(observed, Is.Not.Null);
        Assert.That(observed!.Retry.MaxRetries, Is.EqualTo(1));
    }

    [Test]
    public void BuildServiceClient_applies_RetryDelay_and_RetryMaxDelay_when_set()
    {
        TableClientOptions? observed = null;
        var delay = TimeSpan.FromMilliseconds(40);
        var maxDelay = TimeSpan.FromMilliseconds(400);
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryDelay = delay,
            RetryMaxDelay = maxDelay,
            ConfigureClientOptions = co => observed = co,
        };

        _ = options.BuildServiceClient();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Not.Null);
            Assert.That(observed!.Retry.Delay, Is.EqualTo(delay));
            Assert.That(observed.Retry.MaxDelay, Is.EqualTo(maxDelay));
        });
    }

    [Test]
    public void BuildServiceClient_applies_RetryNetworkTimeout_when_set()
    {
        TableClientOptions? observed = null;
        var networkTimeout = TimeSpan.FromSeconds(7);
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryNetworkTimeout = networkTimeout,
            ConfigureClientOptions = co => observed = co,
        };

        _ = options.BuildServiceClient();

        Assert.That(observed, Is.Not.Null);
        Assert.That(observed!.Retry.NetworkTimeout, Is.EqualTo(networkTimeout));
    }

    [Test]
    public void BuildServiceClient_applies_RetryMode_when_set()
    {
        TableClientOptions? observed = null;
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMode = Azure.Core.RetryMode.Fixed,
            ConfigureClientOptions = co => observed = co,
        };

        _ = options.BuildServiceClient();

        Assert.That(observed, Is.Not.Null);
        Assert.That(observed!.Retry.Mode, Is.EqualTo(Azure.Core.RetryMode.Fixed));
    }

    [Test]
    public void BuildServiceClient_lets_host_ConfigureClientOptions_override_retry_knobs()
    {
        // Ordering contract: knobs are applied first, then the host
        // callback runs and can clobber any of them. Host wins.
        TableClientOptions? observed = null;
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            RetryMaxAttempts = 1,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            ConfigureClientOptions = co =>
            {
                co.Retry.MaxRetries = 9;
                co.Retry.Delay = TimeSpan.FromMilliseconds(999);
                observed = co;
            },
        };

        _ = options.BuildServiceClient();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Not.Null);
            Assert.That(observed!.Retry.MaxRetries, Is.EqualTo(9));
            Assert.That(observed.Retry.Delay, Is.EqualTo(TimeSpan.FromMilliseconds(999)));
        });
    }

    [Test]
    public void BuildServiceClient_leaves_sdk_defaults_when_no_retry_knob_is_set()
    {
        // Additive contract: with all knobs null and no host callback
        // touching Retry, the constructed TableClientOptions.Retry
        // surface matches a fresh TableClientOptions - i.e. the SDK
        // defaults. We snapshot a fresh instance and compare.
        var defaults = new TableClientOptions();

        TableClientOptions? observed = null;
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            ConfigureClientOptions = co => observed = co,
        };

        _ = options.BuildServiceClient();

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Retry.MaxRetries, Is.EqualTo(defaults.Retry.MaxRetries));
            Assert.That(observed.Retry.Delay, Is.EqualTo(defaults.Retry.Delay));
            Assert.That(observed.Retry.MaxDelay, Is.EqualTo(defaults.Retry.MaxDelay));
            Assert.That(observed.Retry.NetworkTimeout, Is.EqualTo(defaults.Retry.NetworkTimeout));
            Assert.That(observed.Retry.Mode, Is.EqualTo(defaults.Retry.Mode));
        });
    }

    [Test]
    public void PhaseTwoCoalescingWindow_defaults_to_five_ms()
    {
        // The default carries the throughput campaign's measured
        // Azure-Tables sweet spot - the highest-impact entry on the
        // library-default-flip ladder. The constant exists so a future
        // re-tune happens in exactly one place.
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.PhaseTwoCoalescingWindow, Is.EqualTo(TimeSpan.FromMilliseconds(5)));
        Assert.That(AzureTableWalStorageOptions.DefaultPhaseTwoCoalescingWindow,
            Is.EqualTo(TimeSpan.FromMilliseconds(5)));
    }

    [Test]
    public void Validate_throws_when_PhaseTwoCoalescingWindow_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_when_PhaseTwoCoalescingWindow_is_zero_or_positive()
    {
        var zero = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCoalescingWindow = TimeSpan.Zero,
        };

        var positive = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(5),
        };

        Assert.Multiple(() =>
        {
            Assert.That(zero.Validate, Throws.Nothing);
            Assert.That(positive.Validate, Throws.Nothing);
        });
    }

    [Test]
    public void PhaseTwoCommitTimeout_defaults_to_three_seconds()
    {
        // The library default bounds the phase-2 commit seam at 3 s so a
        // wedged manifest commit faults instead of stalling the per-shard
        // drain loop indefinitely. null remains an explicit opt-out for the
        // historical unbounded behaviour. The constant exists so a future
        // re-tune happens in exactly one place.
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.PhaseTwoCommitTimeout, Is.EqualTo(TimeSpan.FromSeconds(3)));
        Assert.That(AzureTableWalStorageOptions.DefaultPhaseTwoCommitTimeout,
            Is.EqualTo(TimeSpan.FromSeconds(3)));
    }

    [Test]
    public void Validate_throws_when_PhaseTwoCommitTimeout_is_zero()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCommitTimeout = TimeSpan.Zero,
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_PhaseTwoCommitTimeout_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCommitTimeout = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_when_PhaseTwoCommitTimeout_is_null_or_positive()
    {
        var unset = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCommitTimeout = null,
        };

        var positive = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            PhaseTwoCommitTimeout = TimeSpan.FromSeconds(30),
        };

        Assert.Multiple(() =>
        {
            Assert.That(unset.Validate, Throws.Nothing);
            Assert.That(positive.Validate, Throws.Nothing);
        });
    }

    /// <summary>
    /// Minimal Azure.Core <see cref="Azure.Core.TokenCredential"/>
    /// stand-in for tests that need a non-null credential reference
    /// without any actual auth flow.
    /// </summary>
    private sealed class FakeTokenCredential : Azure.Core.TokenCredential
    {
        public override Azure.Core.AccessToken GetToken(
            Azure.Core.TokenRequestContext requestContext,
            CancellationToken cancellationToken) =>
            new("fake", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<Azure.Core.AccessToken> GetTokenAsync(
            Azure.Core.TokenRequestContext requestContext,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }
}
