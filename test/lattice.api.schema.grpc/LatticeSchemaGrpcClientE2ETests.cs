using Orleans.Lattice.Schema;

// it is still served, unchanged, until the next major version removes it (#4126).

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the typed <see cref="LatticeSchemaApiGrpcClient"/> over
/// a live, co-hosted gRPC server bound to a real Orleans cluster's
/// <see cref="ILatticeSchemaControl"/> facade. Exercises the read/write round
/// trip a remote operator (the explorer Schema tab) drives: set a policy, read it
/// back, run the read-only compliance audit, and probe capabilities - all with a
/// permissive authorizer so the transport succeeds and the facade's own gate is
/// the only guard.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeSchemaGrpcClientE2ETests
{
    private const string Tree = "customers";

    private GrpcSchemaClusterFixture _fixture = null!;
    private GrpcSchemaHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcSchemaClusterFixture();
        await _fixture.InitializeAsync();

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("valid", "{}"u8.ToArray());
        await tree.SetAsync("invalid", "not-json"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(new AllowAllSchemaApiAuthorizer());
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
    public async Task set_then_get_policy_round_trips_over_the_wire()
    {
        await _host.Client.SetPolicyAsync(Tree, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));

        var policy = await _host.Client.GetPolicyAsync(Tree);

        Assert.That(policy, Is.Not.Null);
        Assert.That(policy!.Rules, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task get_policy_returns_null_for_an_ungoverned_tree()
    {
        var policy = await _host.Client.GetPolicyAsync("never-configured");

        Assert.That(policy, Is.Null);
    }

    [Test]
    public async Task probe_capabilities_reports_the_target_tree()
    {
        var capabilities = await _host.Client.ProbeCapabilitiesAsync(Tree);

        Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public async Task get_auth_scheme_is_reachable_over_the_client()
    {
        var schemes = await _host.Client.GetAuthSchemeAsync();

        Assert.That(schemes, Is.Not.Null);
    }
}
