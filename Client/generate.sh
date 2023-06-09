VERSION=0.1.0

rm -r out
docker run --rm -v "${PWD}:/local" --network host -u $(id -u ${USER}):$(id -g ${USER})  openapitools/openapi-generator-cli generate \
-i http://localhost:5042/api/swagger/v1/swagger.json \
-g csharp-netcore \
-o /local/out --additional-properties=packageName=Coflnet.Leaderboard.Client,packageVersion=$VERSION,licenseId=MIT

cd out
sed -i 's/GIT_USER_ID/Coflnet/g' src/Coflnet.Leaderboard.Client/Coflnet.Leaderboard.Client.csproj
sed -i 's/GIT_REPO_ID/Leaderboard/g' src/Coflnet.Leaderboard.Client/Coflnet.Leaderboard.Client.csproj
sed -i 's/>OpenAPI/>Coflnet/g' src/Coflnet.Leaderboard.Client/Coflnet.Leaderboard.Client.csproj

dotnet pack
cp src/Coflnet.Leaderboard.Client/bin/Debug/Coflnet.Leaderboard.Client.*.nupkg ..
