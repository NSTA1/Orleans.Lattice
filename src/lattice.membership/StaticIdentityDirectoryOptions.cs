namespace Orleans.Lattice.Membership;

/// <summary>
/// Configures the in-memory roster surfaced by <see cref="StaticIdentityDirectory"/>:
/// an explicitly-declared set of known principals (users and optional groups) for
/// deployments with no queryable external directory - primarily the reference
/// Basic / environment-variable authorizer, whose valid usernames come from
/// <c>LATTICE_STATE_USER_&lt;name&gt;</c> variables set out-of-band at deploy time.
/// Populate it explicitly via <see cref="AddUser(string, string?)"/> /
/// <see cref="AddGroup(string, string?)"/>, or discover the deployed Basic user
/// ids via <see cref="AddUsersFromEnvironment(string, IStaticRosterEnvironment?)"/>.
/// </summary>
public sealed class StaticIdentityDirectoryOptions
{
    /// <summary>
    /// The default environment-variable prefix under which the reference Basic
    /// authorizer stores each user's credential (mirrors that authorizer's own
    /// default), so <see cref="AddUsersFromEnvironment(string, IStaticRosterEnvironment?)"/>
    /// discovers the same user set. User <c>alice</c> is provisioned as
    /// <c>LATTICE_STATE_USER_alice</c>.
    /// </summary>
    public const string DefaultEnvironmentVariablePrefix = "LATTICE_STATE_USER_";

    /// <summary>
    /// The declared roster of known principals, in declaration order.
    /// <see cref="StaticIdentityDirectory"/> takes an immutable snapshot of this
    /// list at construction; later mutation has no effect on an already-built
    /// provider. When the same id is declared more than once the last entry wins.
    /// </summary>
    public IList<DirectoryPrincipal> Principals { get; } = new List<DirectoryPrincipal>();

    /// <summary>
    /// Adds a user principal to the roster.
    /// </summary>
    /// <param name="id">The exact user id (for the Basic authorizer, the
    /// <c>&lt;name&gt;</c> in <c>LATTICE_STATE_USER_&lt;name&gt;</c>). Must not be
    /// <c>null</c> or empty.</param>
    /// <param name="displayName">An optional human-readable display name;
    /// defaults to <paramref name="id"/> when omitted.</param>
    /// <returns>The same options instance for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="id"/> is <c>null</c> or empty.</exception>
    public StaticIdentityDirectoryOptions AddUser(string id, string? displayName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        Principals.Add(new DirectoryPrincipal(id, displayName ?? id, DirectoryPrincipalKind.User));
        return this;
    }

    /// <summary>
    /// Adds a group principal to the roster.
    /// </summary>
    /// <param name="id">The exact group id. Must not be <c>null</c> or empty.</param>
    /// <param name="displayName">An optional human-readable display name;
    /// defaults to <paramref name="id"/> when omitted.</param>
    /// <returns>The same options instance for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="id"/> is <c>null</c> or empty.</exception>
    public StaticIdentityDirectoryOptions AddGroup(string id, string? displayName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        Principals.Add(new DirectoryPrincipal(id, displayName ?? id, DirectoryPrincipalKind.Group));
        return this;
    }

    /// <summary>
    /// Discovers the deployed Basic user ids by scanning the environment for
    /// variables named <c><paramref name="prefix"/>&lt;id&gt;</c> and adds each
    /// stripped <c>&lt;id&gt;</c> as a user. Only variable <b>names</b> are read;
    /// the values (salted PBKDF2 password hashes) are never touched, so no
    /// credential material is surfaced. Ids-only: no display names or groups are
    /// inferred.
    /// </summary>
    /// <param name="prefix">The environment-variable prefix identifying a Basic
    /// credential. Defaults to <see cref="DefaultEnvironmentVariablePrefix"/>.
    /// Must not be <c>null</c> or empty.</param>
    /// <param name="environment">The environment name source; defaults to the
    /// current process environment (<see cref="ProcessStaticRosterEnvironment"/>)
    /// when <c>null</c>.</param>
    /// <returns>The same options instance for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="prefix"/> is <c>null</c> or empty.</exception>
    public StaticIdentityDirectoryOptions AddUsersFromEnvironment(
        string prefix = DefaultEnvironmentVariablePrefix,
        IStaticRosterEnvironment? environment = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        environment ??= new ProcessStaticRosterEnvironment();

        foreach (var name in environment.GetVariableNames())
        {
            if (name.Length > prefix.Length && name.StartsWith(prefix, StringComparison.Ordinal))
            {
                AddUser(name[prefix.Length..]);
            }
        }

        return this;
    }
}
