# Stage 1: Build, on the build machine's own architecture. The output is portable .NET (no apphost, native SQLite
# libraries for every platform), so one build serves every target platform of a multi-architecture image.
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy csproj and restore dependencies
COPY ["AzsbEmu.Server/AzsbEmu.Server.csproj", "AzsbEmu.Server/"]
RUN dotnet restore "AzsbEmu.Server/AzsbEmu.Server.csproj"

# Copy everything else and build release
COPY . .
WORKDIR "/src/AzsbEmu.Server"
RUN dotnet publish "AzsbEmu.Server.csproj" -c Release -o /app/publish /p:UseAppHost=false
# The data directory, created here so the runtime stage needs no RUN step (no emulation for other architectures).
RUN mkdir -p /app/data

# Stage 2: Runtime (ASP.NET Core: the HTTP management API runs on Kestrel)
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

# Persistent data, writable by the image's non-root user ("app", whose UID the base image sets as APP_UID)
COPY --from=build --chown=app:app /app/data /app/data
USER $APP_UID
VOLUME /app/data

# Listen on all interfaces. The management API listens on 8080 inside the container, so the non-root user works on
# runtimes that don't allow unprivileged ports below 1024 (Docker 20.10+ does; others vary). Publish it as host
# port 80 (-p 80:8080), the port ServiceBusAdministrationClient connects to.
ENV AZSB_EMU_Host=0.0.0.0 \
    AZSB_EMU_Port=5672 \
    AZSB_EMU_HttpPort=8080 \
    AZSB_EMU_DataDirectory=/app/data

# AMQP, and the HTTP management API
EXPOSE 5672 8080

ENTRYPOINT ["dotnet", "AzsbEmu.Server.dll"]
