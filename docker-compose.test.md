# NSQ Test Environment - Docker Compose

This docker-compose configuration sets up the NSQ services required for running the integration tests.

## Services

0. **${HOST_IP}** -- Set your host ip

1. **nsqlookupd** - NSQ Lookup Daemon
   - TCP port: `127.0.0.1:4160` (for nsqd registration)
   - HTTP port: `127.0.0.1:4161` (for client discovery)

2. **nsqd** - Primary NSQ Daemon
   - TCP port: `127.0.0.1:4150` (for Producer/Consumer connections)
   - HTTP port: `127.0.0.1:4151` (for HTTP API operations)
   - Connected to nsqlookupd

3. **nsqd-secondary** - Secondary NSQ Daemon (for ConsumerRdyRedistributionTest)
   - TCP port: `127.0.0.1:5150` (for Producer/Consumer connections)
   - HTTP port: `127.0.0.1:5151` (for HTTP API operations)
   - Connected to nsqlookupd

## Usage

### Start all services:
```bash
docker-compose up -d
```

### Check service status:
```bash
docker-compose ps
```

### View logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f nsqlookupd
docker-compose logs -f nsqd
docker-compose logs -f nsqd-secondary
```

### Stop all services:
```bash
docker-compose down
```

### Stop and remove volumes:
```bash
docker-compose down -v
```

## Running Tests

After starting the services, you can run the integration tests with:
```bash
# Enable integration tests (define RUN_INTEGRATION_TESTS preprocessor symbol)
# Then run your test suite
```

## Notes

- All ports are bound to `127.0.0.1` (localhost only) for security
- Services are configured with health checks to ensure proper startup order
- The nsqd instances register with nsqlookupd automatically
- The broadcast-address is set to `127.0.0.1` so clients connect to the correct address

