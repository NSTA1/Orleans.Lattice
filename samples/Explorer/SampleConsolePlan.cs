namespace Orleans.Lattice.Samples.Explorer;

/// <summary>Where the Explorer console is served, which region's gRPC endpoint it dials and who it signs in as.</summary>
/// <param name="WebPort">The HTTP port the console is served on.</param>
/// <param name="Endpoint">The gRPC endpoint of the region the console connects to.</param>
/// <param name="ConfigPath">The console's persisted configuration file.</param>
/// <param name="SignInAs">The identity the console signs in as automatically, or <see langword="null"/> for none.</param>
internal sealed record SampleConsolePlan(int WebPort, Uri Endpoint, string ConfigPath, string? SignInAs = SampleIdentities.Administrator)
{
    /// <summary>The console's address.</summary>
    public Uri Url => new($"http://localhost:{WebPort}/");
}
