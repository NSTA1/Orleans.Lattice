using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// End-to-end coverage of the tenant directory facade with delegated tenant access
/// administration <b>off</b> (the default): an authorized caller is refused with the
/// typed feature-disabled error and nothing is written, while an unauthorized caller
/// is denied before it can learn the feature's posture.
/// </summary>
/// <remarks>Owned by the epic coordinator's integration run.</remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantDirectoryAdminFeatureOffIntegrationTests
{
    private readonly TenantDirectoryClusterFixture _fixture = new(delegatedAccessEnabled: false);

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task An_authorized_caller_gets_a_typed_refusal_and_nothing_is_written()
    {
        await _fixture.SeedTenantAsync("acme", adminSubjects: "alice");

        foreach (var caller in new[] { "alice", TenantDirectoryClusterFixture.Operator })
        {
            using (TenantDirectoryClusterFixture.As(caller))
            {
                Assert.That(
                    async () => await _fixture.Directory.UpsertGroupAsync("acme", new TenantGroupDescriptor { Name = "eng" }),
                    Throws.TypeOf<TenantAccessAdministrationDisabledException>(),
                    caller);
            }
        }

        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await _fixture.Membership.GetGroupAsync("t/acme/eng"), Is.Null);
        }
    }

    [Test]
    public async Task An_unauthorized_caller_is_denied_before_the_posture_is_revealed()
    {
        await _fixture.SeedTenantAsync("beta", adminSubjects: "ben");

        using (TenantDirectoryClusterFixture.As("mallory"))
        {
            Assert.That(
                async () => await _fixture.Directory.ListGroupsAsync("beta", new TenantAccessPageRequest()),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }
    }
}
