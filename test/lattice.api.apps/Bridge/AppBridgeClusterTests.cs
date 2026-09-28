namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// End to end on a real single-silo cluster: an allowed write through the app bridge lands in the app's
/// structural tree (<c>a/{slug}/{tree}</c>, or <c>t/{tenant}/a/{slug}/{tree}</c> with a tenant active), and a
/// denied operation touches nothing - including a write by a caller whose broad non-app rights would allow it
/// directly but who holds only the app's viewer role.
/// </summary>
/// <remarks>Owned by the epic coordinator's integration run.</remarks>
[TestFixture("")]
[TestFixture("acme")]
[Category("Integration")]
public sealed class AppBridgeClusterTests(string tenant)
{
    private readonly AppBridgeClusterFixture _fixture = new(tenant.Length == 0 ? null : TenantId.Parse(tenant));

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        await _fixture.InitializeAsync();
        await _fixture.InstallAsync();
    }

    [OneTimeTearDown]
    public Task TearDownAsync() => _fixture.DisposeAsync();

    [Test]
    public void The_notes_tree_is_the_structural_tree_of_the_app_in_the_active_tenant() =>
        Assert.That(_fixture.NotesTree, Is.EqualTo(tenant.Length == 0 ? "a/notes-app/notes" : $"t/{tenant}/a/notes-app/notes"));

    [Test]
    public async Task An_allowed_write_lands_in_the_apps_structural_tree()
    {
        using (_fixture.As(AppBridgeClusterFixture.Editor))
        {
            await _fixture.Bridge.SetAsync(_fixture.Target(), "written", new byte[] { 1, 2, 3 });
            var read = await _fixture.Bridge.GetAsync(_fixture.Target(), "written");
            Assert.That(read!.Value.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
            var page = await _fixture.Bridge.ScanAsync(_fixture.Target(), "writ", 10);
            Assert.That(page.Entries.Select(e => e.Key), Does.Contain("written"));
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "written"), Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task An_allowed_delete_removes_the_key_from_the_apps_structural_tree()
    {
        await _fixture.WriteRawAsync(_fixture.NotesTree, "doomed", [9]);

        using (_fixture.As(AppBridgeClusterFixture.Editor))
        {
            Assert.That(await _fixture.Bridge.DeleteAsync(_fixture.Target(), "doomed"), Is.True);
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "doomed"), Is.Null);
    }

    [Test]
    public async Task A_viewers_denied_write_and_delete_touch_nothing()
    {
        await _fixture.WriteRawAsync(_fixture.NotesTree, "kept", [4]);

        using (_fixture.As(AppBridgeClusterFixture.Viewer))
        {
            Assert.That((await _fixture.Bridge.GetAsync(_fixture.Target(), "kept"))!.Value.ToArray(), Is.EqualTo(new byte[] { 4 }));
            AssertDenied(() => _fixture.Bridge.SetAsync(_fixture.Target(), "viewer-write", new byte[] { 5 }));
            AssertDenied(() => _fixture.Bridge.SetAsync(_fixture.Target(), "kept", new byte[] { 5 }));
            AssertDenied(() => _fixture.Bridge.DeleteAsync(_fixture.Target(), "kept"));
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "viewer-write"), Is.Null);
        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "kept"), Is.EqualTo(new byte[] { 4 }));
    }

    [Test]
    public async Task A_caller_with_broad_non_app_rights_but_only_the_viewer_role_is_denied_a_write_through_the_bridge()
    {
        // Precondition: outside the app, the operator's own rights let her write the app's tree directly.
        await Orleans.Lattice.Testing.TestPoll.UntilAsync(
            async () =>
            {
                using (_fixture.As(AppBridgeClusterFixture.Operator))
                {
                    try
                    {
                        await _fixture.Cluster.Client.GetGrain<ILattice>(_fixture.NotesTree).SetAsync("operator-direct", [6]);
                        return true;
                    }
                    catch (LatticeAuthorizationDeniedException)
                    {
                        return false;
                    }
                }
            },
            "the operator's broad rule reaches the data-plane policy",
            timeout: TimeSpan.FromSeconds(60));

        using (_fixture.As(AppBridgeClusterFixture.Operator))
        {
            Assert.That(await _fixture.Bridge.GetAsync(_fixture.Target(), "operator-direct"), Is.Not.Null, "the viewer role still reads");
            AssertDenied(() => _fixture.Bridge.SetAsync(_fixture.Target(), "operator-bridge", new byte[] { 7 }));
            AssertDenied(() => _fixture.Bridge.DeleteAsync(_fixture.Target(), "operator-direct"));
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "operator-bridge"), Is.Null);
        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "operator-direct"), Is.EqualTo(new byte[] { 6 }));
    }

    [Test]
    public async Task A_stale_install_revision_is_denied_and_touches_nothing()
    {
        var stale = _fixture.Target() with { InstallRevision = _fixture.InstallRevision - 1 };

        using (_fixture.As(AppBridgeClusterFixture.Editor))
        {
            AssertDenied(() => _fixture.Bridge.SetAsync(stale, "stale-write", new byte[] { 8 }));
        }

        Assert.That(await _fixture.ReadRawAsync(_fixture.NotesTree, "stale-write"), Is.Null);
    }

    [Test]
    public void An_undeclared_tree_is_not_found()
    {
        using (_fixture.As(AppBridgeClusterFixture.Editor))
        {
            var error = Assert.ThrowsAsync<AppBridgeException>(() => _fixture.Bridge.GetAsync(_fixture.Target("elsewhere"), "k"));
            Assert.That(error!.Failure, Is.EqualTo(AppBridgeFailure.NotFound));
        }
    }

    private static void AssertDenied(Func<Task> call)
    {
        var error = Assert.ThrowsAsync<AppBridgeException>(async () => await call());
        Assert.That(error!.Failure, Is.EqualTo(AppBridgeFailure.Denied));
    }
}
