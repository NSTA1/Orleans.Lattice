namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="ProcessStaticRosterEnvironment"/>: it surfaces the
/// process variable <b>names</b> and never their values (which for a Basic
/// credential is a password hash).
/// </summary>
public class ProcessStaticRosterEnvironmentTests
{
    [Test]
    public void GetVariableNames_includes_a_set_variable_name_but_not_its_value()
    {
        const string name = "LATTICE_STATE_USER_probe_test_only";
        const string hashValue = "pbkdf2-sha256$100000$salt$key-that-must-never-surface";
        Environment.SetEnvironmentVariable(name, hashValue);
        try
        {
            var names = new ProcessStaticRosterEnvironment().GetVariableNames();

            Assert.That(names, Does.Contain(name));
            Assert.That(names, Does.Not.Contain(hashValue));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public void AddUsersFromEnvironment_over_process_env_surfaces_id_only()
    {
        const string name = "LATTICE_STATE_USER_probe_user";
        const string hashValue = "pbkdf2-sha256$100000$salt$secret";
        Environment.SetEnvironmentVariable(name, hashValue);
        try
        {
            var options = new StaticIdentityDirectoryOptions().AddUsersFromEnvironment();

            var probe = options.Principals.SingleOrDefault(p => p.Id == "probe_user");
            Assert.That(probe, Is.Not.Null);
            Assert.That(probe!.DisplayName, Is.EqualTo("probe_user"));
            Assert.That(options.Principals.Any(p => p.DisplayName == hashValue), Is.False);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }
}
