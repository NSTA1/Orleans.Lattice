using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppRoleBindingTests
{
    [Test]
    public void Create_preserves_group_binding_and_record_equality()
    {
        var binding = AppRoleBinding.Create("reader", "group-id");
        Assert.That(binding.RoleName, Is.EqualTo("reader"));
        Assert.That(binding.GroupId, Is.EqualTo("group-id"));
        Assert.That(binding, Is.EqualTo(new AppRoleBinding { RoleName = "reader", GroupId = "group-id" }));
        Assert.That(binding with { GroupId = "another-group" }, Is.Not.EqualTo(binding));
    }

    [TestCase(null, "group-id", "roleName")]
    [TestCase("", "group-id", "roleName")]
    [TestCase("reader", null, "groupId")]
    [TestCase("reader", "", "groupId")]
    public void Create_rejects_missing_identifiers(string? role, string? group, string parameter)
    {
        var error = Assert.Catch<ArgumentException>(() => AppRoleBinding.Create(role!, group!));
        Assert.That(error!.ParamName, Is.EqualTo(parameter));
    }

    [Test]
    public void Orleans_roundtrip_preserves_role_and_group()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppRoleBinding).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var source = AppRoleBinding.Create("reader", "group-id");
        Assert.That(serializer.Deserialize<AppRoleBinding>(serializer.SerializeToArray(source)), Is.EqualTo(source));
    }
}
