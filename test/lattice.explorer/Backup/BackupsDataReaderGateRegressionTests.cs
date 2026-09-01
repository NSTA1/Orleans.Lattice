using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The measured defect this epic's most consequential finding named: the Backups
/// area rendered <em>enabled</em> for <c>data-reader</c>, an identity holding
/// only cluster <c>Read</c> and <c>RangeRead</c> and no backup grant at all.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why that mattered more than a cosmetic slip.</b> An enabled entry is an
/// invitation. The caller followed it and met a server-side denial inside, which
/// is strictly worse than an honest disabled entry: it wastes their time, it
/// teaches them the UI cannot be trusted, and it hides the one thing they needed
/// to know, which is that they lack the Backup permission and who can issue it.
/// </para>
/// <para>
/// <b>Why the old gate got it wrong, and what reproduces it.</b> The coarse gate
/// read "the catalog list call did not throw" as proof of backup access. A
/// cluster that lets a read-only identity page an <em>empty</em> catalog answers
/// that call successfully, so the gate admitted them. Every case here therefore
/// scripts a listing that <b>succeeds</b> while the capability probe reports no
/// list grant - which is precisely the shape of the pre-fix false allow, and
/// which no test asserting only "an unconfigured probe denies" would catch.
/// </para>
/// <para>
/// Nothing here waits on a clock, a timer or a background task: the fake control
/// client answers every call synchronously from scripted values.
/// </para>
/// </remarks>
[TestFixture]
public sealed class BackupsDataReaderGateRegressionTests
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(BackupsPluginKeys.PluginId);

    /// <summary>
    /// The reference-architecture <c>data-reader</c> identity: signed in, holding
    /// cluster read grants, and holding nothing that names backups.
    /// </summary>
    private static IExplorerAuthSession DataReader()
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        return session;
    }

    /// <summary>
    /// A cluster that answers <c>data-reader</c> exactly as the measured one did:
    /// the catalog listing succeeds and returns an empty page, while the
    /// capability probe reports no list grant on any scope.
    /// </summary>
    private static FakeBackupControlClient ClusterAsMeasured() => new()
    {
        ListResult = new BackupCatalogPage(),
        CapabilitiesResult = new BackupScopeCapabilities
        {
            Scope = BackupScopeSelector.WholeTree(BackupCapabilityService.CapabilityProbeTreeId),
            CanList = false,
        },
    };

    [Test]
    public async Task A_reader_whose_empty_catalogue_listing_succeeds_is_not_reported_allowed()
    {
        var client = ClusterAsMeasured();
        var service = new BackupCapabilityService(client, new ExplorerPluginAccessStore(), DataReader());

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(
                access.State,
                Is.Not.EqualTo(ExplorerPluginAccessState.Allowed),
                "a successful listing is not an admission: this is the measured data-reader false allow");
            Assert.That(access.IsAllowed, Is.False);
            Assert.That(
                access.State,
                Is.EqualTo(ExplorerPluginAccessState.Denied),
                "a signed-in caller shown to hold no grant is denied, not invited to sign in again");
        });
    }

    [Test]
    public async Task The_gate_never_reads_the_catalogue_listing_at_all()
    {
        // The stronger statement: the listing is not merely outweighed, it is
        // not consulted. So no future change to what an empty listing returns
        // can reopen the area for an identity with no grant.
        var client = ClusterAsMeasured();
        var service = new BackupCapabilityService(client, new ExplorerPluginAccessStore(), DataReader());

        await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(client.ListCallCount, Is.Zero, "the gate asks the control plane, not the catalogue");
            Assert.That(client.CapabilityProbeCallCount, Is.EqualTo(1));
            Assert.That(
                client.LastProbedScope?.TreeId,
                Is.EqualTo(BackupCapabilityService.CapabilityProbeTreeId),
                "the coarse grant is probed over the reserved whole-tree scope");
        });
    }

    [Test]
    public async Task The_denial_names_the_backup_permission_and_who_issues_it()
    {
        // The remedy is the half that makes the honest denial useful: a disabled
        // entry saying only "Backups is not available" tells the caller what they
        // can already see.
        var client = ClusterAsMeasured();
        var service = new BackupCapabilityService(client, new ExplorerPluginAccessStore(), DataReader());

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.Remedy.IsSpecified, Is.True);
            Assert.That(access.Remedy.Permission, Is.EqualTo("Backup"));
            Assert.That(access.Remedy.Audience, Is.EqualTo("an operator"));
            Assert.That(access.Remedy.Describe(), Does.Contain("Backup").And.Contain("operator"));
        });
    }

    [Test]
    public async Task A_reader_who_can_list_one_tree_still_reaches_the_area()
    {
        // The converse, so the fix is a correction rather than a blanket
        // closure: an identity the cluster grants list access on a real scope
        // must still reach Backups even without the cluster-wide grant.
        var client = ClusterAsMeasured();
        client.CapabilitiesByTree["orders"] = new BackupScopeCapabilities
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
            CanList = true,
        };

        var store = new ExplorerPluginAccessStore();
        var service = new BackupCapabilityService(client, store, DataReader());

        await service.ProbeScopeAsync("orders");
        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
    }

    [Test]
    public async Task A_reader_whose_only_scope_grant_is_revoked_loses_the_area_again()
    {
        // The scope grant is re-derived from the store on every probe rather
        // than latched, so revoking it closes the area rather than leaving a
        // remembered admission behind - the same class of stale evidence the
        // original defect rested on.
        var client = ClusterAsMeasured();
        client.CapabilitiesByTree["orders"] = new BackupScopeCapabilities
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
            CanList = true,
        };

        var store = new ExplorerPluginAccessStore();
        var service = new BackupCapabilityService(client, store, DataReader());
        await service.ProbeScopeAsync("orders");
        var whileGranted = await service.ProbeAsync(Context);

        client.CapabilitiesByTree["orders"] = new BackupScopeCapabilities
        {
            Scope = BackupScopeSelector.WholeTree("orders"),
            CanList = false,
        };
        await service.ProbeScopeAsync("orders");
        var afterRevoke = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(whileGranted.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(afterRevoke.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
        });
    }

    [Test]
    public async Task A_denied_area_is_demoted_rather_than_hidden_so_the_reader_can_ask_for_the_grant()
    {
        // The whole point of an honest denial: the caller must still be able to
        // see that a Backups area exists, or they cannot ask anyone for the
        // permission that would open it.
        var client = ClusterAsMeasured();
        var service = new BackupCapabilityService(client, new ExplorerPluginAccessStore(), DataReader());

        var access = await service.ProbeAsync(Context);
        var presentation = ExplorerAreaVisibilityPolicy.Decide(access.State, hideInaccessible: false);

        Assert.Multiple(() =>
        {
            Assert.That(presentation, Is.EqualTo(ExplorerAreaEntryPresentation.Demoted));
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(access.State),
                Is.False,
                "the cluster serves backups; this caller is the one without the grant");
        });
    }

    [Test]
    public async Task An_unimplemented_backup_facade_is_still_reported_unavailable_rather_than_denied()
    {
        // The precedence rule the contract owns, pinned from this area: a
        // capability the cluster does not run outranks the caller's credential,
        // so it must not be reported as something they lack a grant for.
        var client = ClusterAsMeasured();
        client.CapabilitiesThrows = new RpcException(new Status(StatusCode.Unimplemented, "no backup control"));
        var service = new BackupCapabilityService(client, new ExplorerPluginAccessStore(), DataReader());

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
    }
}
