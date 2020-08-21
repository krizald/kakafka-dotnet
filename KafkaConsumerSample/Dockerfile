FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build
WORKDIR /KafkaConsumerSample

# copy csproj and restore as distinct layers
COPY KafkaConsumerSample/*.csproj .
RUN dotnet restore

# copy and publish app and libraries
COPY KafkaConsumerSample/ .
RUN dotnet build -c release -o /app --no-restore

# # final stage/image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "KafkaConsumerSample.dll"]