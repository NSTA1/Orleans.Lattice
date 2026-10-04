using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The optional half of the area contract: every member an area may leave
/// unwritten has a default, and each default is the one the chrome can safely
/// assume of an area that says nothing - tenant-scoped, one chain node per
/// segment, no completions, no commands, no Home line, no badge.
/// </summary>
/// <remarks>
/// <para>
/// A default interface member is only ever reached through an implementor that
/// does not override it, so a fixture that writes its own full implementation
/// proves nothing about the default. <see cref="SilentArea"/> therefore declares
/// exactly the three required members and nothing else, and
/// <see cref="TalkativeArea"/> is its contrast: every default overridden, so a
/// default silently hard-wired to a constant would redden here rather than pass
/// in both directions.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ExplorerAreaDefaultMemberTests
{
    private static readonly ExplorerAddress Address = ExplorerAddress.ForArea("probe", "x", "y");

    [Test]
    public void An_area_that_says_nothing_is_tenant_scoped()
    {
        IExplorerArea area = new SilentArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.IsTenantScoped, Is.True);
            Assert.That(area.IsTenantScopedAt(Address), Is.True, "the per-address answer defaults to the area-wide one");
        });
    }

    [Test]
    public void The_per_address_answer_follows_the_area_wide_one_rather_than_a_constant()
    {
        // IsTenantScopedAt defaults to `IsTenantScoped`, not to `true`. An area
        // that overrides only the area-wide flag must see the per-address answer
        // move with it, which is the single thing this default can get wrong.
        IExplorerArea clusterWide = new ClusterWideArea();

        Assert.Multiple(() =>
        {
            Assert.That(clusterWide.IsTenantScoped, Is.False);
            Assert.That(clusterWide.IsTenantScopedAt(Address), Is.False);
        });
    }

    [Test]
    public void An_area_that_says_nothing_groups_its_path_one_node_per_segment()
    {
        IExplorerArea area = new SilentArea();

        Assert.That(area.GetChainSpans(Address), Is.Null, "null is how the address line asks for one node per segment");
    }

    [Test]
    public void An_area_that_says_nothing_offers_no_completions()
    {
        IExplorerArea area = new SilentArea();

        Assert.That(area.Completions, Is.Null);
    }

    [Test]
    public void An_area_that_says_nothing_contributes_no_commands()
    {
        IExplorerArea area = new SilentArea();

        Assert.That(area.Commands, Is.Empty);
    }

    [Test]
    public async Task An_area_that_says_nothing_reports_no_home_status()
    {
        IExplorerArea area = new SilentArea();

        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task An_area_that_says_nothing_reports_no_directory_badge()
    {
        IExplorerArea area = new SilentArea();

        Assert.That(await area.GetDirectoryBadgeAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task Every_default_is_a_default_and_not_a_hard_wired_answer()
    {
        // The battery: an overriding area must be able to answer differently on
        // every one of them. Without this each test above would also pass against
        // a member the interface had sealed to its default value.
        IExplorerArea area = new TalkativeArea();

        Assert.Multiple(async () =>
        {
            Assert.That(area.IsTenantScoped, Is.False);
            Assert.That(area.IsTenantScopedAt(Address), Is.True);
            Assert.That(area.GetChainSpans(Address), Is.EqualTo(new[] { 2 }));
            Assert.That(area.Completions, Is.Not.Null);
            Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { "probe.go" }));
            Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("1 thing"));
            Assert.That(await area.GetDirectoryBadgeAsync(CancellationToken.None), Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task The_chrome_reads_the_defaults_through_the_directory()
    {
        // The members above are reached directly; this proves the directory - the
        // only caller of the Home-status and badge defaults - also lands on them
        // rather than short-circuiting an area that overrides nothing.
        var directory = new ExplorerAreaDirectory(
            [new SilentArea()],
            new ExplorerChromeOptions(),
            TimeProvider.System);
        var area = directory.Areas.Single();

        Assert.Multiple(async () =>
        {
            Assert.That(await directory.GetHomeStatusAsync(area), Is.Null);
            Assert.That(await directory.GetDirectoryBadgeAsync(area), Is.Null);
            Assert.That((await directory.GetEntriesAsync()).Single().Badge, Is.Null);
        });
    }

    /// <summary>An area declaring only the three required members, so every optional one is the interface default.</summary>
    private sealed class SilentArea : IExplorerArea
    {
        public string Key => "probe";

        public string DisplayName => "Probe";

        public int DirectoryOrder => 0;

        public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(AreaAvailability.Visible);
    }

    /// <summary>An area that overrides only the area-wide tenant flag, to prove the per-address default follows it.</summary>
    private sealed class ClusterWideArea : IExplorerArea
    {
        public string Key => "cluster";

        public string DisplayName => "Cluster";

        public int DirectoryOrder => 1;

        public bool IsTenantScoped => false;

        public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(AreaAvailability.Visible);
    }

    /// <summary>An area that overrides every optional member, as the contrast to <see cref="SilentArea"/>.</summary>
    private sealed class TalkativeArea : IExplorerArea
    {
        public string Key => "talkative";

        public string DisplayName => "Talkative";

        public int DirectoryOrder => 2;

        public bool IsTenantScoped => false;

        public bool IsTenantScopedAt(ExplorerAddress address) => true;

        public IReadOnlyList<int>? GetChainSpans(ExplorerAddress address) => [2];

        public IAddressCompletionSource? Completions => new NoCompletions();

        public IReadOnlyList<ExplorerCommand> Commands => [new("probe.go", "Go")];

        public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(AreaAvailability.Visible);

        public ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>("1 thing");

        public ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>("1");
    }

    /// <summary>A completion source that completes nothing; NSubstitute cannot proxy this internal interface.</summary>
    private sealed class NoCompletions : IAddressCompletionSource
    {
        public ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken) =>
            ValueTask.FromResult<IReadOnlyList<AddressCompletion>>([]);
    }
}
