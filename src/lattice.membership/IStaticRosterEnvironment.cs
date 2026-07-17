namespace Orleans.Lattice.Membership;

/// <summary>
/// Enumerates environment-variable <i>names</i> for
/// <see cref="StaticIdentityDirectoryOptions.AddUsersFromEnvironment(string, IStaticRosterEnvironment?)"/>,
/// so the deployed Basic user set (each user provisioned as a
/// <c>LATTICE_STATE_USER_&lt;name&gt;</c> credential) can be surfaced as a browsable
/// roster. Only variable <b>names</b> are exposed - never their values - so a
/// user's PBKDF2 password hash can never be read, stored, or surfaced through
/// this seam. Abstracted so the roster population can be unit-tested against an
/// in-memory name set without mutating real process environment state.
/// </summary>
public interface IStaticRosterEnvironment
{
    /// <summary>
    /// Returns the names of every environment variable currently defined. The
    /// caller filters these by prefix; the underlying values are never read.
    /// </summary>
    /// <returns>The defined environment-variable names.</returns>
    IReadOnlyCollection<string> GetVariableNames();
}

/// <summary>
/// The default <see cref="IStaticRosterEnvironment"/>, backed by the current
/// process environment. Reads only the variable <b>names</b> via
/// <see cref="Environment.GetEnvironmentVariables()"/>; the values (which for the
/// Basic authorizer are salted PBKDF2 password hashes) are never touched.
/// </summary>
public sealed class ProcessStaticRosterEnvironment : IStaticRosterEnvironment
{
    /// <inheritdoc />
    public IReadOnlyCollection<string> GetVariableNames()
    {
        var variables = Environment.GetEnvironmentVariables();
        var names = new List<string>(variables.Count);
        foreach (System.Collections.DictionaryEntry entry in variables)
        {
            if (entry.Key is string name)
            {
                names.Add(name);
            }
        }

        return names;
    }
}
