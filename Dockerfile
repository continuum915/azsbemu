# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy csproj and restore dependencies
COPY ["AzsbEmu.Server/AzsbEmu.Server.csproj", "AzsbEmu.Server/"]
RUN dotnet restore "AzsbEmu.Server/AzsbEmu.Server.csproj"

# Copy everything else and build release
COPY . .
WORKDIR "/src/AzsbEmu.Server"
RUN dotnet publish "AzsbEmu.Server.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

# Configure defaults and persistent volume
ENV AZSB_EMU_DATA_DIR=/app/data
VOLUME /app/data

# Expose standard AMQP port
EXPOSE 5672

ENTRYPOINT ["dotnet", "AzsbEmu.Server.dll"]
