using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder(args);

// The standalone web head is built on the same embeddable extensions the
// Orleans.Lattice.Explorer.Web hosting library exposes, so the standalone head
// and any co-hosted explorer share one code path and cannot drift.
builder.Services.AddLatticeExplorerWeb();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();

app.MapLatticeExplorer();

app.Run();
