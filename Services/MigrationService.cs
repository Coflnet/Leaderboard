using ISession = Cassandra.ISession;
using Coflnet.Cassandra;
using Cassandra;
using Prometheus;
using StackExchange.Redis;
using System.Text;
using Cassandra.Data.Linq;
using Coflnet.Leaderboard.Models;

namespace Coflnet.Leaderboard.Services;

public class MigrationService : BackgroundService
{
    private LeaderboardService leaderboardService;
    private ILogger<MigrationService> logger;
    private ISession oldSession;
    private ISession newSession;
    private readonly ConnectionMultiplexer redis;
    Counter migrated = Metrics.CreateCounter("leaderboard_migration_migrated", "The number of items migrated");

    public MigrationService(LeaderboardService leaderboardService, OldSession oldSession, ISession session, ILogger<MigrationService> logger, ConnectionMultiplexer redis)
    {
        this.leaderboardService = leaderboardService;
        this.logger = logger;
        this.oldSession = oldSession.Session;
        this.newSession = session;
        this.redis = redis;
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
        var offset = 0;
        var db = redis.GetDatabase();
        var fromRedis = db.StringGet("leaderboard_migration_offset");
        if (!fromRedis.IsNullOrEmpty)
        {
            offset = int.Parse(fromRedis);
        }
        var table = new Table<BoardScore>(oldSession);
        var newTable = new Table<BoardScore>(newSession);
        statement = new SimpleStatement("SELECT * FROM boardscore");
        Console.WriteLine("Starting migration from offset {0}", offset);

        var scores = await table.ExecuteAsync();
        foreach (var batch in Batch(scores.Skip(offset), 200))
        {
            var batchStatement = new BatchStatement();
            foreach (var score in batch)
            {
                batchStatement.Add(newTable.Insert(score));
            }
            await newSession.ExecuteAsync(batchStatement);
            migrated.Inc(batch.Count());
            offset += batch.Count();
            db.StringSet("leaderboard_migration_offset", offset);
            // free up memory
            Console.WriteLine("Migrated batch {0}", offset);
        }
        logger.LogInformation("Migrated scores");
        // cql for selecting columns on table: SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'leaderboard' AND table_name = 'bucket';
    }

    private async Task InsertScore(Row score)
    {
        await newSession.ExecuteAsync(new SimpleStatement("INSERT INTO boardscore (slug, bucketid, score, userid, confidence, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            score.GetValue<string>("slug"), score.GetValue<long>("bucketid"), score.GetValue<long>("score"), score.GetValue<string>("userid"), score.GetValue<short>("confidence"), score.GetValue<DateTime>("timestamp")));
    }

    private IEnumerable<IEnumerable<T>> Batch<T>(IEnumerable<T> source, int size)
    {
        List<T> batch = new List<T>(size);
        foreach (var item in source)
        {
            batch.Add(item);
            if (batch.Count == size)
            {
                yield return batch;
                batch = new List<T>(size);
            }
        }
        if (batch.Count > 0)
        {
            yield return batch;
        }
    }
}