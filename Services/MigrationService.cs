using ISession = Cassandra.ISession;
using Coflnet.Cassandra;
using Cassandra;
using Prometheus;
using StackExchange.Redis;
using System.Text;
using Cassandra.Data.Linq;
using Coflnet.Leaderboard.Models;
using Coflnet.Core;
using Cassandra.Mapping;

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
        var offset = 0;
        var db = redis.GetDatabase();
        var fromRedis = db.StringGet("leaderboard_migration_offset");
        if (!fromRedis.IsNullOrEmpty)
        {
            offset = int.Parse(fromRedis);
        }
        var table = new Table<BoardScore>(oldSession);
        var newTable = new Table<BoardScore>(newSession);
        var statement = new SimpleStatement("SELECT * FROM boardscore");
        Console.WriteLine("Starting migration from offset {0}", offset);

        var query = table;
        query.SetAutoPage(false);
        query.SetPageSize(1000);
        var pagingSateRedis = db.StringGet("leaderboard_migration_paging_state");
        byte[] ?pagingState = null;
        if (!pagingSateRedis.IsNullOrEmpty)
        {
            pagingState = Convert.FromBase64String(pagingSateRedis);
            query.SetPagingState(pagingState);
        }
        var scores = await query.ExecutePagedAsync();
        do
        {

            var batchStatement = new BatchStatement();
            foreach (var score in scores)
            {
                batchStatement.Add(newTable.Insert(score));
            }
            await newSession.ExecuteAsync(batchStatement);
            migrated.Inc(scores.Count());
            offset += scores.Count();
            db.StringSet("leaderboard_migration_offset", offset);
            var queryState = scores.PagingState;
            if (queryState != null)
            {
                db.StringSet("leaderboard_migration_paging_state", Convert.ToBase64String(queryState));
            }
            pagingState = queryState;
            // free up memory
            Console.WriteLine("Migrated batch {0}", offset);
            await Task.Delay(100);

        } while ((scores = await GetNextPage(query,pagingState)) != null);
        logger.LogInformation("Migrated scores");
        // cql for selecting columns on table: SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'leaderboard' AND table_name = 'bucket';
    }

    private static async Task<IPage<BoardScore>> GetNextPage(Table<BoardScore> query, byte[]? pagingState)
    {
        if (pagingState == null)
            return null;
        query.SetPagingState(pagingState);
        return await query.ExecutePagedAsync();
    }
}