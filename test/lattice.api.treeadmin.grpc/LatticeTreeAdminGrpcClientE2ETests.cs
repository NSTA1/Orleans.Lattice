namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the typed <see cref="LatticeTreeAdminApiGrpcClient"/> over
/// a live, co-hosted gRPC server bound to a real Orleans cluster's
/// <see cref="ILatticeTreeAdmin"/> facade. At this scaffolding stage the facade
/// exposes the capability probe (composing the wrapped schema facade) and the
/// unauthenticated auth-scheme discovery RPC; this fixture drives both over the wire
/// with a permissive authorizer so the transport succeeds and the facade's own gate
/// is the only guard.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminGrpcClientE2ETests
{
    private const string Tree = "customers";

    private GrpcTreeAdminClusterFixture _fixture = null!;
    private GrpcTreeAdminHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
        await _fixture.InitializeAsync();

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k", "{}"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(new AllowAllTreeAdminApiAuthorizer());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task probe_capabilities_reports_the_target_tree_and_composed_schema()
    {
        var capabilities = await _host.Client.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
            // Composed schema capabilities ride along, keyed to the same tree.
            Assert.That(capabilities.Schema, Is.Not.Null);
            Assert.That(capabilities.Schema.TreeId, Is.EqualTo(Tree));
            // Scaffolding stage: no whole-tree admin gate exists yet.
            Assert.That(capabilities.CanAdministerTree, Is.False);
        });
    }

    [Test]
    public async Task get_auth_scheme_is_reachable_over_the_client()
    {
        var schemes = await _host.Client.GetAuthSchemeAsync();

        Assert.That(schemes, Is.Not.Null);
    }
}
