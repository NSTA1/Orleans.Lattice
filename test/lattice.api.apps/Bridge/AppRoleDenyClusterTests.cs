using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// End to end on a real single-silo cluster (coordinator review of #3902): an app role is held by binding, but an
/// explicit deny rule on a bound member still refuses the app bridge. The bridge never consults the caller's own
/// rules to grant (steps 1-4 of <see cref="LatticeAppBridge"/>); its deny semantics come from step 5, where the
/// data-path call runs under the caller's own identity and the core access-gate enforcement refuses it, which the
/// bridge translates to <see cref="AppBridgeFailure.Denied"/>. That was already so before #3902 and is unchanged.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppRoleDenyClusterTests
{
    private const string TreeDenied = "dora";
    private const string ClusterDenied = "cora";

    private static readonly LatticeOperation AllData =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete;

    private readonly AppBridgeClusterFixture _fixture = new(tenant: null, allTreesGrants: true);

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        await _fixture.InitializeAsync();
        await _fixture.InstallAsync();

        var store = _fixture.Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        var directory = _fixture.Silo.GetRequiredService<ILatticeMembershipDirectory>();
        using (LatticeSystemOrigin.Enter())
        {
            await store.PutRuleAsync(new LatticeAuthorizationRule(
                "deny-dora-notes", LatticeSubjectSelector.User(TreeDenied), LatticeScope.Tree(_fixture.NotesTree), AllData, LatticeEffect.Deny));
            await store.PutRuleAsync(new LatticeAuthorizationRule(
                "deny-cora-everywhere", LatticeSubjectSelector.User(ClusterDenied), LatticeScope.Tree(LatticeScope.ClusterWideTreeId), AllData, LatticeEffect.Deny));
            await directory.AddMemberAsync(AppBridgeClusterFixture.Editors, TreeDenied);
            await directory.AddMemberAsync(AppBridgeClusterFixture.Editors, ClusterDenied);
        }
    }

    [OneTimeTearDown]
    public Task TearDownAsync() => _fixture.DisposeAsync();

    [TestCase(TreeDenied)]
    [TestCase(ClusterDenied)]
    public async Task A_bound_editor_with_an_explicit_deny_is_refused_every_bridge_verb(string subject)
    {
        // Both halves must be live before the bridge is judged: the membership (the workspace reports the bound
        // role) and the deny (a direct data-path write is refused).
        await TestPoll.UntilAsync(
            async () => (await RolesAsync(subject)).Contains("editor") && !await CanWriteDirectlyAsync(subject),
            "the binding and the deny both reach the silo",
            timeout: TimeSpan.FromSeconds(60));
        await _fixture.WriteRawAsync(_fixture.NotesTree, "deny-kept-" + subject, [4]);

        using (_fixture.As(subject))
        {
            // The data plane hides a denied read rather than throwing: a point read reports the key absent
            // (LatticeGrain.IsPointReadAllowedAsync) and a range read filters every key out. The bridge passes that
            // through, so the stored value is never disclosed.
            Assert.That(await _fixture.Bridge.GetAsync(_fixture.Target(), "deny-kept-" + subject), Is.Null);
            Assert.That((await _fixture.Bridge.ScanAsync(_fixture.Target(), "deny-", 10)).Entries, Is.Empty);
            AssertDenied(() => _fixture.Bridge.SetAsync(_fixture.Target(), "deny-write-" + subject, new byte[] { 1 }));
            AssertDenied(() => _fixture.Bridge.DeleteAsync(_fixture.Target(), "deny-kept-" + subject));
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "deny-write-" + subject), Is.Null);
            Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "deny-kept-" + subject), Is.EqualTo(new byte[] { 4 }));
        });
    }

    [Test]
    public async Task A_deny_on_one_member_leaves_the_other_bound_editors_their_writes()
    {
        using (_fixture.As(AppBridgeClusterFixture.Editor))
        {
            await _fixture.Bridge.SetAsync(_fixture.Target(), "deny-unaffected", new byte[] { 2 });
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "deny-unaffected"), Is.EqualTo(new byte[] { 2 }));
    }

    private async Task<string[]> RolesAsync(string subject)
    {
        var workspace = _fixture.Silo.GetRequiredService<ILatticeAppWorkspace>();
        using (_fixture.As(subject))
        {
            return (await workspace.ListMyAppsAsync()).SelectMany(app => app.Roles).ToArray();
        }
    }

    private async Task<bool> CanWriteDirectlyAsync(string subject)
    {
        using (_fixture.As(subject))
        {
            try
            {
                await _fixture.Cluster.Client.GetGrain<ILattice>(_fixture.NotesTree).SetAsync("deny-probe-" + subject, [1]);
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }

    private static void AssertDenied(Func<Task> call)
    {
        var error = Assert.ThrowsAsync<AppBridgeException>(async () => await call());
        Assert.That(error!.Failure, Is.EqualTo(AppBridgeFailure.Denied));
    }
}
