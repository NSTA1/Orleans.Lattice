using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// membership add-on, and a JWT authenticator trusting an in-test symmetric
/// signing key. Tokens are minted in-process (see <see cref="MintToken"/>); no
/// live identity provider and no network are involved. Shared by the membership
/// integration tests.
/// </summary>
public sealed class MembershipClusterFixture
{
    /// <summary>The issuer the fixture's JWT authenticator trusts.</summary>
    public const string Issuer = "https://issuer.membership.test/";

    /// <summary>The audience the fixture's JWT authenticator accepts.</summary>
    public const string Audience = "lattice-membership-tests";

    private static readonly SymmetricSecurityKey SigningKey =
        new(Encoding.UTF8.GetBytes("membership-integration-signing-key-0123456789"));

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider (source of the silo-side membership services).</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side membership directory.</summary>
    public ILatticeMembershipDirectory Directory =>
        SiloServices.GetRequiredService<ILatticeMembershipDirectory>();

    /// <summary>The silo-side membership context.</summary>
    public ILatticeMembershipContext Context =>
        SiloServices.GetRequiredService<ILatticeMembershipContext>();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Mints a signed JWT for the fixture's issuer / audience so a test can stamp
    /// it as the ambient credential.
    /// </summary>
    /// <param name="subject">The subject id to assert.</param>
    /// <param name="groups">Optional token-asserted group ids.</param>
    /// <param name="expires">Optional explicit expiry; defaults to one hour out.</param>
    public static string MintToken(string subject, IEnumerable<string>? groups = null, DateTime? expires = null)
    {
        var claims = new List<Claim> { new("sub", subject) };
        if (groups is not null)
        {
            foreach (var group in groups)
            {
                claims.Add(new Claim("groups", group));
            }
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Audience = Audience,
            Subject = new ClaimsIdentity(claims),
            Expires = expires ?? DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeJwtAuthenticator(options =>
            {
                options.Issuer = Issuer;
                options.SchemeHint = "Bearer";
                options.Audiences.Add(Audience);
                options.SigningKeys.Add(SigningKey);
            });
        }
    }
}
