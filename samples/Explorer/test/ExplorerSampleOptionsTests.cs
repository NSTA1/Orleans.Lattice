using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class ExplorerSampleOptionsTests
{
    private static readonly Func<string, string?> NoEnvironment = _ => null;

    private static ExplorerSampleOptions Parse(params string[] args) => Parse(NoEnvironment, args);

    private static ExplorerSampleOptions Parse(Func<string, string?> environment, params string[] args)
    {
        Assert.That(ExplorerSampleOptions.TryParse(args, environment, out var options, out var error), Is.True, error);
        return options!;
    }

    private static string Reject(params string[] args) => Reject(NoEnvironment, args);

    private static string Reject(Func<string, string?> environment, params string[] args)
    {
        Assert.That(ExplorerSampleOptions.TryParse(args, environment, out var options, out var error), Is.False);
        Assert.That(options, Is.Null);
        return error!;
    }

    [Test]
    public void No_arguments_run_the_estate_connected_to_east_on_the_default_ports()
    {
        var options = Parse();

        Assert.That(options.Minimal, Is.False);
        Assert.That(options.ExplorerRegion, Is.EqualTo(SampleIdentities.EastRegion));
        Assert.That(options.StartPeerPaused, Is.False);
        Assert.That(options.Entra, Is.Null);
        Assert.That(options.GroupMergeMode, Is.EqualTo(SubjectGroupMergeMode.Union));
        Assert.That(options.Ports, Is.EqualTo(SamplePorts.Default));
        Assert.That(options.WriterInterval, Is.EqualTo(TimeSpan.FromSeconds(1)));
        Assert.That(options.ExplorerConfigPath, Does.EndWith("explorer-sample-config.json"));
    }

    [Test]
    public void Minimal_switch_selects_the_single_region_run() =>
        Assert.That(Parse("--minimal").Minimal, Is.True);

    [Test]
    public void Switches_are_case_insensitive() =>
        Assert.That(Parse("--MINIMAL").Minimal, Is.True);

    [TestCase("west", "west")]
    [TestCase("East", "east")]
    [TestCase(" WEST ", "west")]
    public void Explorer_region_switch_selects_the_region(string value, string expected) =>
        Assert.That(Parse("--explorer-region", value).ExplorerRegion, Is.EqualTo(expected));

    [Test]
    public void Explorer_region_switch_without_a_value_is_rejected() =>
        Assert.That(Reject("--explorer-region"), Does.Contain("needs a region"));

    [Test]
    public void Explorer_region_switch_with_an_unknown_region_is_rejected() =>
        Assert.That(Reject("--explorer-region", "north"), Does.Contain("'north' is not a region"));

    [Test]
    public void Sign_in_as_switch_picks_the_automatic_identity() =>
        Assert.That(Parse("--sign-in-as", "acme-admin").SignInAs, Is.EqualTo(SampleIdentities.AcmeAdmin));

    [TestCase("none")]
    [TestCase("NONE")]
    public void Sign_in_as_none_turns_the_automatic_sign_in_off(string value) =>
        Assert.That(Parse("--sign-in-as", value).SignInAs, Is.Null);

    [Test]
    public void The_console_signs_in_as_the_administrator_by_default() =>
        Assert.That(Parse().SignInAs, Is.EqualTo(SampleIdentities.Administrator));

    [TestCase("--sign-in-as")]
    [TestCase("--sign-in-as", "")]
    [TestCase("--sign-in-as", "alice:secret")]
    [TestCase("--sign-in-as", "two words")]
    [TestCase("--sign-in-as", "--minimal")]
    public void Sign_in_as_without_a_usable_user_is_rejected(params string[] args) =>
        Assert.That(Reject(args), Does.Contain("--sign-in-as needs a user name"));

    [Test]
    public void Peer_paused_switch_is_read() =>
        Assert.That(Parse("--peer-paused").StartPeerPaused, Is.True);

    [Test]
    public void Port_offset_switch_shifts_every_port() =>
        Assert.That(Parse("--port-offset", "7").Ports, Is.EqualTo(SamplePorts.Default.Offset(7)));

    [TestCase("")]
    [TestCase("-1")]
    [TestCase("seven")]
    [TestCase("30001")]
    public void Port_offset_outside_the_range_is_rejected(string value) =>
        Assert.That(Reject("--port-offset", value), Does.Contain("--port-offset needs a whole number"));

    [Test]
    public void Port_offset_without_a_value_is_rejected() =>
        Assert.That(Reject("--port-offset"), Does.Contain("--port-offset needs a whole number"));

    [Test]
    public void Minimal_with_the_west_region_is_rejected() =>
        Assert.That(Reject("--minimal", "--explorer-region", "west"), Does.Contain("cannot be combined"));

    [Test]
    public void Minimal_with_a_paused_peer_is_rejected() =>
        Assert.That(Reject("--minimal", "--peer-paused"), Does.Contain("cannot be combined"));

    [Test]
    public void Minimal_with_the_east_region_is_accepted() =>
        Assert.That(Parse("--minimal", "--explorer-region", "east").Minimal, Is.True);

    [Test]
    public void An_unknown_argument_is_rejected_with_the_usage() =>
        Assert.That(Reject("--verbose"), Does.Contain("'--verbose' is not recognised").And.Contain(ExplorerSampleOptions.Usage));

    [Test]
    public void All_three_entra_variables_select_the_entra_directory()
    {
        var environment = Environment(
            (ExplorerSampleOptions.EntraTenantIdVariable, "tenant"),
            (ExplorerSampleOptions.EntraClientIdVariable, "client"),
            (ExplorerSampleOptions.EntraClientSecretVariable, "secret"));

        Assert.That(Parse(environment).Entra, Is.EqualTo(new SampleEntraDirectory("tenant", "client", "secret")));
    }

    [Test]
    public void A_half_configured_entra_directory_is_rejected()
    {
        var environment = Environment((ExplorerSampleOptions.EntraTenantIdVariable, "tenant"));

        Assert.That(Reject(environment), Does.Contain("half-configured"));
    }

    [TestCase("TokenOnly", SubjectGroupMergeMode.TokenOnly)]
    [TestCase("directoryonly", SubjectGroupMergeMode.DirectoryOnly)]
    [TestCase("  ", SubjectGroupMergeMode.Union)]
    public void The_merge_mode_variable_is_read(string value, SubjectGroupMergeMode expected) =>
        Assert.That(Parse(Environment((ExplorerSampleOptions.MergeModeVariable, value))).GroupMergeMode, Is.EqualTo(expected));

    [TestCase("Sometimes")]
    [TestCase("42")]
    public void An_unknown_merge_mode_is_rejected(string value) =>
        Assert.That(Reject(Environment((ExplorerSampleOptions.MergeModeVariable, value))), Does.Contain("is not recognised"));

    [Test]
    public void Null_arguments_throw()
    {
        Assert.That(() => ExplorerSampleOptions.TryParse(null!, NoEnvironment, out _, out _), Throws.ArgumentNullException);
        Assert.That(() => ExplorerSampleOptions.TryParse([], null!, out _, out _), Throws.ArgumentNullException);
    }

    private static Func<string, string?> Environment(params (string Name, string Value)[] variables)
    {
        var map = variables.ToDictionary(variable => variable.Name, variable => variable.Value, StringComparer.Ordinal);
        return name => map.GetValueOrDefault(name);
    }
}
