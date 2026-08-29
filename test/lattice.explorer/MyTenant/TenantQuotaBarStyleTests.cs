using Orleans.Lattice.Explorer.MyTenant;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The pre-composed bar-fill widths: correct for every whole percentage, and
/// handed out rather than built, so the quota-polling path allocates nothing on
/// render.
/// </summary>
[TestFixture]
public sealed class TenantQuotaBarStyleTests
{
    [Test]
    public void Every_whole_percentage_composes_its_own_width()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaBarStyle.Width(0), Is.EqualTo("width:0%"));
            Assert.That(TenantQuotaBarStyle.Width(1), Is.EqualTo("width:1%"));
            Assert.That(TenantQuotaBarStyle.Width(25), Is.EqualTo("width:25%"));
            Assert.That(TenantQuotaBarStyle.Width(100), Is.EqualTo("width:100%"));
        });
    }

    [Test]
    public void A_percentage_outside_the_range_is_clamped_rather_than_throwing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaBarStyle.Width(-5), Is.EqualTo("width:0%"));
            Assert.That(TenantQuotaBarStyle.Width(500), Is.EqualTo("width:100%"));
            Assert.That(TenantQuotaBarStyle.Width(int.MinValue), Is.EqualTo("width:0%"));
            Assert.That(TenantQuotaBarStyle.Width(int.MaxValue), Is.EqualTo("width:100%"));
        });
    }

    [Test]
    public void The_same_instance_is_handed_out_so_a_render_allocates_nothing() =>
        Assert.That(TenantQuotaBarStyle.Width(42), Is.SameAs(TenantQuotaBarStyle.Width(42)));

    [Test]
    public void The_width_is_invariant_regardless_of_the_ambient_culture()
    {
        var previous = Thread.CurrentThread.CurrentCulture;
        try
        {
            // A culture using a different digit shape or separator must not leak
            // into an inline CSS declaration, which the browser parses ordinally.
            Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("ar-SA");

            Assert.That(TenantQuotaBarStyle.Width(75), Is.EqualTo("width:75%"));
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = previous;
        }
    }
}
