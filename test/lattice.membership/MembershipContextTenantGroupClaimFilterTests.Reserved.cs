using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// The cost of an always-active filter: the tenancy add-on keeps the filter active
/// whatever its flag (epic #4154, D2), so a tenancy-registered host pays for it on
/// every cold resolution. That cost is one ordinal prefix test per claim-derived
/// group and nothing allocated unless a <c>t/</c> id is actually present, and the
/// warm (cached) path never reaches the filter at all.
/// </summary>
public sealed partial class MembershipContextTenantGroupClaimFilterTests
{
    [Test]
    public void ApplyTenantGroupClaimFilter_active_with_no_claim_derived_tenant_group_allocates_nothing()
    {
        var context = CreateContext(new ExpansionDirectory([], new Dictionary<string, string[]>()), new TenantGroupClaimFilter(static () => true));
        var subject = new LatticeSubject(
            "alice",
            new HashSet<string>(StringComparer.Ordinal) { "t/acme/members", "cluster-readers", "entra-sales" },
            null);
        IReadOnlyCollection<string> directoryGroups = new HashSet<string>(StringComparer.Ordinal) { "t/acme/members" };

        var growth = AllocationProbe.Growth(
            prepare: _ => context,
            measure: (ctx, size) =>
            {
                for (var i = 0; i < size; i++)
                {
                    AllocationProbe.ScalarSink += ctx.ApplyTenantGroupClaimFilter(subject, directoryGroups).GroupIds.Count;
                }
            },
            smallSize: 16,
            largeSize: 1024);

        Assert.Multiple(() =>
        {
            Assert.That(growth, Is.Zero, "the active path scans with a prefix test and allocates only when there is a t/ id to strip");
            Assert.That(context.ApplyTenantGroupClaimFilter(subject, directoryGroups).GroupIds, Is.SameAs(subject.GroupIds));
        });
    }

    [Test]
    public void ApplyTenantGroupClaimFilter_active_strips_a_claim_derived_tenant_group_from_an_array_group_set()
    {
        var context = CreateContext(new ExpansionDirectory([], new Dictionary<string, string[]>()), new TenantGroupClaimFilter(static () => true));
        var subject = new LatticeSubject("alice", new[] { "cluster-readers", "t/acme/finance" }, null);

        var result = context.ApplyTenantGroupClaimFilter(subject, Array.Empty<string>());

        Assert.That(result.GroupIds, Is.EquivalentTo(new[] { "cluster-readers" }));
    }

    [Test]
    public async Task Active_filter_is_not_consulted_on_a_warm_resolution()
    {
        var filter = new CountingActiveFilter();
        var context = CreateContext(
            new ExpansionDirectory([], new Dictionary<string, string[]>()),
            filter,
            assertedGroups: ["t/acme/finance", "cluster-readers"]);

        var cold = await ResolveAsync(context);
        var reads = filter.IsActiveReads;
        var warm = await ResolveAsync(context);

        Assert.Multiple(() =>
        {
            Assert.That(cold.GroupIds, Does.Not.Contain("t/acme/finance"));
            Assert.That(reads, Is.EqualTo(1), "one IsActive read per cold resolution");
            Assert.That(filter.IsActiveReads, Is.EqualTo(reads), "a cache hit never reaches the filter");
            Assert.That(warm.GroupIds, Is.SameAs(cold.GroupIds), "the warm path serves the cached subject");
        });
    }

    /// <summary>An always-active filter that counts IsActive reads and delegates to the real filter.</summary>
    private sealed class CountingActiveFilter : ITenantGroupClaimFilter
    {
        private readonly TenantGroupClaimFilter _inner = new(static () => true);

        public int IsActiveReads { get; private set; }

        public bool IsActive
        {
            get
            {
                IsActiveReads++;
                return true;
            }
        }

        public void Filter(ICollection<string> assertedGroups) => _inner.Filter(assertedGroups);
    }
}
