using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Tests.Domain;

/// <summary>
/// Verifies that every <see cref="ChaosPreset"/> value has a
/// human-readable display name + description surfaced by
/// <see cref="ChaosPresetInfo"/> (used by the chaos fly-out).
/// </summary>
[TestFixture]
public sealed class ChaosPresetInfoTests
{
    [Test]
    public void All_enumerates_every_preset_value()
    {
        var expected = Enum.GetValues<ChaosPreset>();
        Assert.That(ChaosPresetInfo.All, Is.EquivalentTo(expected));
    }

    [Test]
    public void GetDisplayName_returns_non_empty_for_every_preset()
    {
        foreach (var preset in Enum.GetValues<ChaosPreset>())
        {
            var name = ChaosPresetInfo.GetDisplayName(preset);
            Assert.That(name, Is.Not.Null.And.Not.Empty, $"Preset {preset} has no display name");
            Assert.That(name, Is.Not.EqualTo(preset.ToString()),
                $"Preset {preset} display name should be human-friendly, not the enum identifier");
        }
    }

    [Test]
    public void GetDescription_returns_non_empty_for_every_preset()
    {
        foreach (var preset in Enum.GetValues<ChaosPreset>())
        {
            var desc = ChaosPresetInfo.GetDescription(preset);
            Assert.That(desc, Is.Not.Null.And.Not.Empty, $"Preset {preset} has no description");
        }
    }

    [Test]
    public void LatticeStorageFlakes_description_calls_out_divergence()
    {
        var desc = ChaosPresetInfo.GetDescription(ChaosPreset.LatticeStorageFlakes);
        Assert.That(desc, Does.Contain("divergence").IgnoreCase);
    }

    [Test]
    public void ClearAll_description_mentions_reset()
    {
        var desc = ChaosPresetInfo.GetDescription(ChaosPreset.ClearAll);
        Assert.That(desc, Does.Contain("Reset").IgnoreCase);
    }
}
