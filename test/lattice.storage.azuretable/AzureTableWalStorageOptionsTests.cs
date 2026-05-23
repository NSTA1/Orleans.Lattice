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
    public void EliminateCandidateRowOnHotPath_defaults_to_false()
    {
        // Pin the default so callers continue to receive the legacy
        // two-phase WAL contract (C-row written inline; reconciliation
        // discovers orphans via the manifest-partition C-row scan).
        // Variant D is opt-in only - existing deployments must change
        // nothing to keep their current crash-recovery semantics.
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.EliminateCandidateRowOnHotPath, Is.False);
    }

    [Test]
    public void EliminateCandidateRowOnHotPath_round_trips_when_set_to_true()
    {
        var options = new AzureTableWalStorageOptions
        {
            EliminateCandidateRowOnHotPath = true,
        };

        Assert.That(options.EliminateCandidateRowOnHotPath, Is.True);
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
