using ISession = Cassandra.ISession;
using Coflnet.Cassandra;
using Cassandra;
using Prometheus;

namespace Coflnet.Leaderboard.Services;

public class MigrationService : BackgroundService
{
    private LeaderboardService leaderboardService;
    private ILogger<MigrationService> logger;
    private ISession oldSession;
    private ISession newSession;
    Counter migrated = Metrics.CreateCounter("leaderboard_migration_migrated", "The number of items migrated");

    public MigrationService(LeaderboardService leaderboardService, OldSession oldSession, ISession session, ILogger<MigrationService> logger)
    {
        this.leaderboardService = leaderboardService;
        this.logger = logger;
        this.oldSession = oldSession.Session;
        this.newSession = session;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting migration");
        await leaderboardService.Create();
        // move from old session to new session
        var statement = new SimpleStatement("SELECT * FROM bucket");
        // set page size to 1000
        statement.SetPageSize(1000);
        var buckets = await oldSession.ExecuteAsync(statement);
        foreach (var bucket in buckets)
        {
            // copy persist original insert time
            await newSession.ExecuteAsync(new SimpleStatement("INSERT INTO bucket (slug, bucketid, minimumscore) VALUES (?, ?, ?)", bucket.GetValue<string>("slug"), bucket.GetValue<long>("bucketid"), bucket.GetValue<long>("minimumscore")));
            Console.Write("\rMigrated bucket {0} ", bucket.GetValue<string>("slug"));
        }
        logger.LogInformation("Migrated buckets");

        statement = new SimpleStatement("SELECT * FROM boardscore");
        statement.SetPageSize(1000);
        var scores = await oldSession.ExecuteAsync(statement);
        await Parallel.ForEachAsync(scores,
            new ParallelOptions { MaxDegreeOfParallelism = 30 },
        async (score, c) =>
        {
            await newSession.ExecuteAsync(new SimpleStatement("INSERT INTO boardscore (slug, bucketid, score, userid, confidence, timestamp) VALUES (?, ?, ?, ?, ?, ?)", 
                score.GetValue<string>("slug"), score.GetValue<long>("bucketid"), score.GetValue<long>("score"), score.GetValue<string>("userid"), score.GetValue<short>("confidence"), score.GetValue<DateTime>("timestamp")));
            migrated.Inc();
        });
        logger.LogInformation("Migrated scores");
        // cql for selecting columns on table: SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'leaderboard' AND table_name = 'bucket';
    }
}