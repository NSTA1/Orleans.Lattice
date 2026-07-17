namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="StaticIdentityDirectoryOptions"/>: the explicit
/// user / group builders, environment-prefix population (ids only, never the
/// credential value), and argument guards.
/// </summary>
public class StaticIdentityDirectoryOptionsTests
{
    [Test]
    public void AddUser_defaults_display_name_to_id()
    {
        var options = new StaticIdentityDirectoryOptions().AddUser("alice");

        Assert.That(options.Principals, Has.Count.EqualTo(1));
        Assert.That(options.Principals[0], Is.EqualTo(new DirectoryPrincipal("alice", "alice", DirectoryPrincipalKind.User)));
    }

    [Test]
    public void AddUser_uses_supplied_display_name()
    {
        var options = new StaticIdentityDirectoryOptions().AddUser("alice", "Alice Smith");

        Assert.That(options.Principals[0].DisplayName, Is.EqualTo("Alice Smith"));
        Assert.That(options.Principals[0].Kind, Is.EqualTo(DirectoryPrincipalKind.User));
    }

    [Test]
    public void AddGroup_adds_a_group_principal()
    {
        var options = new StaticIdentityDirectoryOptions().AddGroup("admins", "Administrators");

        Assert.That(options.Principals[0], Is.EqualTo(new DirectoryPrincipal("admins", "Administrators", DirectoryPrincipalKind.Group)));
    }

    [Test]
    public void AddGroup_defaults_display_name_to_id()
    {
        var options = new StaticIdentityDirectoryOptions().AddGroup("admins");

        Assert.That(options.Principals[0].DisplayName, Is.EqualTo("admins"));
    }

    [Test]
    public void AddUser_returns_same_instance_for_chaining()
    {
        var options = new StaticIdentityDirectoryOptions();

        Assert.That(options.AddUser("a"), Is.SameAs(options));
        Assert.That(options.AddGroup("g"), Is.SameAs(options));
    }

    [Test]
    public void AddUser_null_id_throws()
    {
        Assert.That(() => new StaticIdentityDirectoryOptions().AddUser(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddUser_empty_id_throws()
    {
        Assert.That(() => new StaticIdentityDirectoryOptions().AddUser(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void AddGroup_null_id_throws()
    {
        Assert.That(() => new StaticIdentityDirectoryOptions().AddGroup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddGroup_empty_id_throws()
    {
        Assert.That(() => new StaticIdentityDirectoryOptions().AddGroup(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void AddUsersFromEnvironment_adds_only_prefixed_ids_as_users()
    {
        var environment = new FakeRosterEnvironment(
            "LATTICE_STATE_USER_alice",
            "LATTICE_STATE_USER_bob",
            "PATH",
            "SOME_OTHER_VAR");

        var options = new StaticIdentityDirectoryOptions().AddUsersFromEnvironment(environment: environment);

        Assert.That(options.Principals.Select(p => p.Id), Is.EquivalentTo(new[] { "alice", "bob" }));
        Assert.That(options.Principals.All(p => p.Kind == DirectoryPrincipalKind.User), Is.True);
    }

    [Test]
    public void AddUsersFromEnvironment_ids_are_display_names_never_the_credential_value()
    {
        // The env value would be a PBKDF2 hash in a real deployment; the source
        // only exposes names, so no value can ever be surfaced.
        var environment = new FakeRosterEnvironment("LATTICE_STATE_USER_alice");

        var options = new StaticIdentityDirectoryOptions().AddUsersFromEnvironment(environment: environment);

        var principal = options.Principals.Single();
        Assert.That(principal.Id, Is.EqualTo("alice"));
        Assert.That(principal.DisplayName, Is.EqualTo("alice"));
        Assert.That(principal.Claims, Is.Null);
    }

    [Test]
    public void AddUsersFromEnvironment_ignores_the_bare_prefix_with_no_id()
    {
        var environment = new FakeRosterEnvironment("LATTICE_STATE_USER_");

        var options = new StaticIdentityDirectoryOptions().AddUsersFromEnvironment(environment: environment);

        Assert.That(options.Principals, Is.Empty);
    }

    [Test]
    public void AddUsersFromEnvironment_honours_a_custom_prefix()
    {
        var environment = new FakeRosterEnvironment("APP_USER_carol", "LATTICE_STATE_USER_dave");

        var options = new StaticIdentityDirectoryOptions().AddUsersFromEnvironment("APP_USER_", environment);

        Assert.That(options.Principals.Select(p => p.Id), Is.EqualTo(new[] { "carol" }));
    }

    [Test]
    public void AddUsersFromEnvironment_returns_same_instance_for_chaining()
    {
        var options = new StaticIdentityDirectoryOptions();

        Assert.That(options.AddUsersFromEnvironment(environment: new FakeRosterEnvironment()), Is.SameAs(options));
    }

    [Test]
    public void AddUsersFromEnvironment_null_prefix_throws()
    {
        Assert.That(
            () => new StaticIdentityDirectoryOptions().AddUsersFromEnvironment(null!, new FakeRosterEnvironment()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddUsersFromEnvironment_empty_prefix_throws()
    {
        Assert.That(
            () => new StaticIdentityDirectoryOptions().AddUsersFromEnvironment(string.Empty, new FakeRosterEnvironment()),
            Throws.ArgumentException);
    }

    [Test]
    public void DefaultEnvironmentVariablePrefix_matches_the_basic_authorizer_convention()
    {
        Assert.That(StaticIdentityDirectoryOptions.DefaultEnvironmentVariablePrefix, Is.EqualTo("LATTICE_STATE_USER_"));
    }
}
