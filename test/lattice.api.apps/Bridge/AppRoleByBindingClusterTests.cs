using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// End to end on a real single-silo cluster (issue #3902): an app role is held by binding, not by capability.
/// The operator holds broad rights of her own over the app's tree but is bound only to the viewer role, so the
/// app workspace - which drives the roles the frame reports - says viewer only, and the bridge agrees by
/// denying her writes. Bound viewers and editors hold exactly their own roles.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppRoleByBindingClusterTests
{
    private readonly AppBridgeClusterFixture _fixture = new(tenant: null);

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        await _fixture.InitializeAsync();
        await _fixture.InstallAsync();
    }

    [OneTimeTearDown]
    public Task TearDownAsync() => _fixture.DisposeAsync();

    [Test]
    public async Task A_broad_rights_viewer_holds_viewer_only_and_the_bridge_agrees()
    {
        // Precondition: outside the app, the operator's own rule lets her write the app's tree directly.
        await TestPoll.UntilAsync(
            () => CanWriteDirectlyAsync(AppBridgeClusterFixture.Operator),
            "the operator's broad rule reaches the data-plane policy",
            timeout: TimeSpan.FromSeconds(60));

        var workspace = _fixture.Silo.GetRequiredService<ILatticeAppWorkspace>();
        using (_fixture.As(AppBridgeClusterFixture.Operator))
        {
            var app = (await workspace.ListMyAppsAsync()).Single();
            var described = await workspace.DescribeMyAppAsync(AppBridgeClusterFixture.Slug);

            Assert.Multiple(() =>
            {
                Assert.That(app.Roles, Is.EqualTo(new[] { "viewer" }), "broad rights of her own never add the editor role");
                Assert.That(described!.Roles.Select(role => role.Name), Is.EqualTo(new[] { "viewer" }));
            });

            var denied = Assert.ThrowsAsync<AppBridgeException>(() => _fixture.Bridge.SetAsync(_fixture.Target(), "b4-operator", new byte[] { 1 }));
            Assert.That(denied!.Failure, Is.EqualTo(AppBridgeFailure.Denied), "the bridge honours exactly the reported roles");
        }
    }

    [TestCase(AppBridgeClusterFixture.Viewer, "viewer")]
    [TestCase(AppBridgeClusterFixture.Editor, "editor")]
    public async Task A_bound_member_holds_exactly_its_own_role(string subject, string role)
    {
        var workspace = _fixture.Silo.GetRequiredService<ILatticeAppWorkspace>();
        using (_fixture.As(subject))
        {
            Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { role }));
        }
    }

    [Test]
    public async Task A_caller_bound_to_no_role_sees_no_app()
    {
        var workspace = _fixture.Silo.GetRequiredService<ILatticeAppWorkspace>();
        using (_fixture.As("nobody"))
        {
            Assert.That(await workspace.ListMyAppsAsync(), Is.Empty);
        }
    }

    private async Task<bool> CanWriteDirectlyAsync(string subject)
    {
        using (_fixture.As(subject))
        {
            try
            {
                await _fixture.Cluster.Client.GetGrain<ILattice>(_fixture.NotesTree).SetAsync("b4-direct-" + subject, [1]);
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }
}
