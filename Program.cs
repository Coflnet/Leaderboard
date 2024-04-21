using Coflnet.Core;
using Coflnet.Leaderboard.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddCoflnetCore();
builder.Services.AddSingleton<LeaderboardService>();
builder.Services.AddSingleton<ConnectionMultiplexer>(ConnectionMultiplexer.Connect(builder.Configuration["REDIS_HOST"]));
if(builder.Configuration["OLD_CASSANDRA:HOSTS"] != null)
{
    Console.WriteLine("Migrating from old cassandra");
    builder.Services.AddHostedService<MigrationService>();
}

var app = builder.Build();


app.UseCoflnetCore();

app.UseAuthorization();

app.MapControllers();

app.UseSwagger(a =>
{
    a.RouteTemplate = "api/swagger/{documentName}/swagger.json";
});
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/api/swagger/v1/swagger.json", "SkyApi v1");
    c.RoutePrefix = "api";
});

app.Run();
