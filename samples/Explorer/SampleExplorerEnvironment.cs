using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The Explorer console's bootstrap variables, held by the sample rather than
/// written into the process environment. The console's first-run seed reads the
/// endpoint and the automatic sign-in through <see cref="IExplorerEnvironment"/>,
/// so the sample can choose the region and the identity per run, and a test host
/// in the same process sees nothing it did not set.
/// </summary>
internal sealed class SampleExplorerEnvironment : IExplorerEnvironment
{
    private readonly Dictionary<string, string> _variables;

    /// <summary>Seeds the console to dial <paramref name="endpoint"/> over loopback h2c and sign in as <paramref name="signInAs"/>.</summary>
    /// <param name="endpoint">The region's gRPC endpoint.</param>
    /// <param name="signInAs">The identity to sign in as automatically, or <see langword="null"/> for none.</param>
    public SampleExplorerEnvironment(Uri endpoint, string? signInAs = SampleIdentities.Administrator)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        _variables = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [EnvironmentExplorerBootstrap.EndpointVariable] = endpoint.GetLeftPart(UriPartial.Authority),
            [EnvironmentExplorerBootstrap.InsecureDevVariable] = "true",
        };

        if (!string.IsNullOrWhiteSpace(signInAs))
        {
            // The sample's authenticator never checks the password.
            _variables[EnvironmentExplorerBootstrap.UsernameVariable] = signInAs;
            _variables[EnvironmentExplorerBootstrap.PasswordVariable] = SampleIdentities.AdministratorPassword;
        }
    }

    /// <inheritdoc />
    public string? GetVariable(string name) => _variables.GetValueOrDefault(name);
}
