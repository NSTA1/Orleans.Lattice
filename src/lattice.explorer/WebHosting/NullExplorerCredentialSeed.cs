using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// The web head's default <see cref="IExplorerCredentialSeed"/>: it seeds nothing.
/// </summary>
/// <remarks>
/// <para>
/// The environment credential seed exists for a single-operator launcher: it
/// applies <c>LATTICE_EXPLORER_USERNAME</c> / <c>LATTICE_EXPLORER_PASSWORD</c> in
/// memory when the credential store is empty. That is safe in the desktop head,
/// where one process serves exactly one operator, but the web head is multi-user:
/// its <see cref="CookieCredentialStore"/> is per browser, so it is empty for
/// every anonymous visitor. Seeding there would sign each of them in with the
/// process-wide operator credential, handing full cluster authority to anyone who
/// can reach the page and re-globalising the per-circuit auth state the Explorer
/// deliberately scopes.
/// </para>
/// <para>
/// Registering this seed keeps the (secret-free) endpoint seed working while
/// withholding the credential seed. A single-operator web deployment can opt the
/// credential seed back in with
/// <see cref="LatticeExplorerWebOptions.AllowEnvironmentCredentialSeed"/>.
/// </para>
/// </remarks>
internal sealed class NullExplorerCredentialSeed : IExplorerCredentialSeed
{
    /// <inheritdoc />
    public StoredCredential? TrySeed() => null;
}
