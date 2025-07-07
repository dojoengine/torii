# Grafana Setup for Torii Metrics

This guide explains how to set up Grafana to visualize Torii indexer metrics using Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- Torii running with metrics enabled
- Access to the Torii metrics endpoint (default: localhost:9200)

## Quick Start

1. **Start Torii with metrics enabled:**
   ```bash
   torii --metrics --metrics.addr 0.0.0.0 --metrics.port 9200
   ```

2. **Start Grafana and Prometheus:**
   ```bash
   docker-compose -f docker-compose.grafana.yml up -d
   ```

3. **Access Grafana:**
   - Open http://localhost:3000 in your browser
   - Login with username: `admin`, password: `admin`
   - Navigate to the "Torii Indexer Overview" dashboard

## Architecture

The setup includes:

- **Prometheus** (port 9090): Scrapes metrics from Torii
- **Grafana** (port 3000): Visualizes metrics with pre-configured dashboards

## Dashboard Overview

The "Torii Indexer Overview" dashboard provides comprehensive monitoring of:

### Indexer Engine Metrics
- **Fetch Duration**: Time taken to fetch data from the blockchain
- **Process Duration**: Time taken to process fetched events
- **Fetch/Process Rates**: Success and error rates for operations
- **Backoff Delays**: Current backoff delays when errors occur
- **Events Processed**: Rate of events processed by contract type

### Fetcher Metrics
- **RPC Duration**: Time taken for individual RPC calls
- **RPC Request Rates**: Success and error rates for RPC requests
- **Throughput**: Events fetched and blocks processed rates
- **Error Rates**: Combined error rates across all operations

## Configuration

### Prometheus Configuration

The Prometheus configuration (`grafana/prometheus.yml`) scrapes metrics from Torii:

```yaml
scrape_configs:
  - job_name: 'torii-indexer'
    static_configs:
      - targets: ['host.docker.internal:9200']
    scrape_interval: 5s
```

### Grafana Provisioning

Grafana is automatically configured with:
- Prometheus as the default data source
- Pre-built dashboards in the "Torii" folder
- Admin user with password "admin"

## Customization

### Adding New Dashboards

1. Create new dashboard JSON files in `grafana/dashboards/`
2. Restart Grafana: `docker-compose -f docker-compose.grafana.yml restart grafana`

### Modifying Data Sources

Edit `grafana/provisioning/datasources/prometheus.yml` to add or modify data sources.

### Changing Grafana Settings

Modify environment variables in `docker-compose.grafana.yml`:

```yaml
environment:
  - GF_SECURITY_ADMIN_PASSWORD=your_password
  - GF_USERS_ALLOW_SIGN_UP=false
  - GF_SERVER_ROOT_URL=http://your-domain.com
```

## Troubleshooting

### Grafana Can't Connect to Prometheus

1. Ensure Prometheus is running: `docker-compose -f docker-compose.grafana.yml logs prometheus`
2. Check Prometheus targets: http://localhost:9090/targets
3. Verify Torii metrics endpoint: http://localhost:9200/metrics

### No Data in Dashboards

1. Verify Torii is running with metrics enabled
2. Check that metrics are being exposed: `curl http://localhost:9200/metrics`
3. Ensure Prometheus is scraping successfully: http://localhost:9090/targets

### Dashboard Not Loading

1. Check Grafana logs: `docker-compose -f docker-compose.grafana.yml logs grafana`
2. Verify dashboard files are in the correct location
3. Restart Grafana to reload provisioning

## Metrics Reference

### Indexer Engine Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `torii_indexer_fetch_duration_seconds` | Histogram | Time taken to fetch data |
| `torii_indexer_process_duration_seconds` | Histogram | Time taken to process events |
| `torii_indexer_fetch_total` | Counter | Total fetch operations |
| `torii_indexer_process_total` | Counter | Total process operations |
| `torii_indexer_backoff_delay_seconds` | Gauge | Current backoff delay |
| `torii_indexer_events_processed_total` | Counter | Events processed by type |
| `torii_indexer_errors_total` | Counter | Errors by operation |

### Fetcher Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `torii_fetcher_rpc_duration_seconds` | Histogram | RPC call duration |
| `torii_fetcher_rpc_requests_total` | Counter | RPC requests by status |
| `torii_fetcher_events_fetched_total` | Counter | Total events fetched |
| `torii_fetcher_blocks_to_fetch_total` | Counter | Blocks queued for fetching |
| `torii_fetcher_errors_total` | Counter | Fetcher errors by operation |

## Production Considerations

- Change default Grafana admin password
- Configure proper authentication and authorization
- Set up persistent storage for Grafana data
- Configure alerting for critical metrics
- Use external Prometheus for production deployments
