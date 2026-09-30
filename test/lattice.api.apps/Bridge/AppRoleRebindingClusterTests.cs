using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// End to end on a real single-silo cluster (issue #3884): re-binding an enabled app's role to
/// another membership group replaces its compiled rules, so a member of the old group loses the
/// role's grants and a member of the new group gains them, while an untouched binding keeps its
/// grants and the app stays enabled.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppRoleRebindingClusterTests
{
    private const string NewEditors = "g-notes-editors-next";
    private const string NewEditor = "nina";

    private readonly AppBridgeClusterFixture _fixture = new(tenant: null);

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        await _fixture.InitializeAsync();
        await _fixture.InstallAsync();

        var directory = _fixture.Silo.GetRequiredService<ILatticeMembershipDirectory>();
        using (LatticeSystemOrigin.Enter())
        {
            await directory.UpsertGroupAsync(new MembershipGroup(NewEditors));
            await directory.AddMemberAsync(NewEditors, NewEditor);
        }
    }

    [OneTimeTearDown]
    public Task TearDownAsync() => _fixture.DisposeAsync();

    [Test]
    public async Task Rebinding_a_role_moves_its_grants_from_the_old_group_to_the_new_one()
    {
        await UntilAsync(() => CanWriteAsync(AppBridgeClusterFixture.Editor), true, "the bound editor writes before the re-binding");
        Assert.That(await CanWriteAsync(NewEditor), Is.False, "a member of the new group holds nothing before the re-binding");

        var rebinder = _fixture.Silo.GetRequiredService<ILatticeAppRoleBindings>();
        AppRoleBindingsReport report;
        using (_fixture.As(AppBridgeClusterFixture.BootstrapAdmin))
        {
            report = await rebinder.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate
            {
                Slug = AppBridgeClusterFixture.Slug,
                Version = AppBridgeClusterFixture.Version,
                RoleBindings =
                [
                    new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = AppBridgeClusterFixture.Viewers },
                    new AppRoleBindingDescriptor { RoleName = "editor", GroupId = NewEditors },
                ],
            });
        }

        Assert.Multiple(() =>
        {
            Assert.That(report.State, Is.EqualTo(AppLifecycleState.Enabled), "a re-binding never changes the lifecycle state");
            Assert.That(report.RoleBindings.Single(binding => binding.RoleName == "editor").GroupId, Is.EqualTo(NewEditors));
        });

        await UntilAsync(() => CanWriteAsync(NewEditor), true, "a member of the new group gains the editor role's grants");
        await UntilAsync(() => CanWriteAsync(AppBridgeClusterFixture.Editor), false, "a member of the old group loses the editor role's grants");
        Assert.That(await CanReadAsync(AppBridgeClusterFixture.Viewer), Is.True, "the untouched viewer binding keeps its grants");

        // #3902: the workspace's role report follows the binding, not the caller's rights.
        await UntilAsync(async () => (await RolesAsync(NewEditor)).SequenceEqual(["editor"]), true, "the workspace reports the new group's member as editor");
        await UntilAsync(async () => (await RolesAsync(AppBridgeClusterFixture.Editor)).Length == 0, true, "the workspace no longer reports the old group's member");
        Assert.That(await RolesAsync(AppBridgeClusterFixture.Viewer), Is.EqualTo(new[] { "viewer" }));
    }

    private async Task<string[]> RolesAsync(string subject)
    {
        var workspace = _fixture.Silo.GetRequiredService<ILatticeAppWorkspace>();
        using (_fixture.As(subject))
        {
            return (await workspace.ListMyAppsAsync()).SelectMany(app => app.Roles).ToArray();
        }
    }

    private async Task<bool> CanWriteAsync(string subject)
    {
        using (_fixture.As(subject))
        {
            try
            {
                await _fixture.Cluster.Client.GetGrain<ILattice>(_fixture.NotesTree).SetAsync("rebind-" + subject, [1]);
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }

    private async Task<bool> CanReadAsync(string subject)
    {
        using (_fixture.As(subject))
        {
            try
            {
                await _fixture.Cluster.Client.GetGrain<ILattice>(_fixture.NotesTree).GetAsync("rebind-probe");
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }

    private static Task UntilAsync(Func<Task<bool>> probe, bool expected, string what) =>
        TestPoll.UntilAsync(async () => await probe() == expected, what, timeout: TimeSpan.FromSeconds(60));
}
