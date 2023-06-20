using Coflnet.Leaderboard.Client.Api;

var boardName = "test-loadtest"+DateTime.Now.Ticks;
Console.WriteLine($"Testing leaderboard {boardName}");
var client = new ScoresApi("http://localhost:5042");

await InsertScoreRange(boardName, client, -10, 110);

var scores = await client.ScoresLeaderboardSlugGetAsync(boardName);
Console.WriteLine($"Topscore: {scores[1].Score} from {scores[1].UserId} (should be 98)");
if(scores[0].UserId != "99" || scores[1].UserId != "98")
{
    Console.WriteLine("1) Test failed");
    return;
}


await InsertScoreRange(boardName, client, 101, 900);
var lowScores = await client.ScoresLeaderboardSlugGetAsync(boardName, 1000);
if(lowScores[0].UserId != "1000" || lowScores[1].UserId != "999")
{
    Console.WriteLine("2) Test failed");
    Console.WriteLine($"Topscore: {lowScores[1].Score} from {lowScores[1].UserId} (should be 999)");
    return;
}


Console.WriteLine("Test passed");

static async Task InsertScoreRange(string boardName, ScoresApi client, int start, int count)
{
    await Parallel.ForEachAsync(Enumerable.Range(start, count), async (i, cancel) =>
    {
        await client.ScoresLeaderboardSlugPostAsync(boardName, new()
        {
            Score = i,
            UserId = i.ToString()
        });
        if(i % 10 != 0)
        {
            return;
        }
        await client.ScoresLeaderboardSlugPostAsync(boardName, new()
        {
            Score = i + 1,
            UserId = i.ToString()
        });
    });
}