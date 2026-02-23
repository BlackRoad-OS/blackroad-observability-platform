"""Full observability platform - metrics, traces, and logs."""
import sqlite3
import uuid
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import argparse


DB_PATH = Path.home() / ".blackroad" / "observability.db"

# Pre-defined BlackRoad services
SERVICES = ["gateway", "worlds-worker", "dashboard-api", "agents-status", "fleet-manager"]


@dataclass
class Metric:
    """Observability metric."""
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    type: str = "gauge"  # counter, gauge, histogram


@dataclass
class Span:
    """Distributed trace span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    service: str
    operation: str
    start_ts: datetime = field(default_factory=datetime.now)
    end_ts: Optional[datetime] = None
    status: str = "ok"  # ok, error
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class LogEntry:
    """Log entry with trace context."""
    service: str
    level: str  # debug, info, warn, error, fatal
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    trace_id: Optional[str] = None
    fields: Dict[str, Any] = field(default_factory=dict)


class ObservabilityPlatform:
    """Full observability platform."""
    
    def __init__(self):
        self._init_db()
        self._populate_sample_data()
    
    def _init_db(self):
        """Initialize SQLite database."""
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    value REAL,
                    labels TEXT,
                    timestamp TEXT,
                    type TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS spans (
                    trace_id TEXT NOT NULL,
                    span_id TEXT PRIMARY KEY,
                    parent_span_id TEXT,
                    service TEXT,
                    operation TEXT,
                    start_ts TEXT,
                    end_ts TEXT,
                    status TEXT,
                    tags TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id TEXT PRIMARY KEY,
                    service TEXT,
                    level TEXT,
                    message TEXT,
                    timestamp TEXT,
                    trace_id TEXT,
                    fields TEXT
                )
            """)
            conn.commit()
    
    def _populate_sample_data(self):
        """Populate sample data for BlackRoad services."""
        # Add sample metrics for each service
        for service in SERVICES:
            self.record_metric(f"{service}.requests_per_minute", 
                             150 + hash(service) % 100, 
                             {"service": service}, "gauge")
            self.record_metric(f"{service}.error_rate", 
                             (hash(service) % 5) / 100.0, 
                             {"service": service}, "gauge")
            self.record_metric(f"{service}.p99_latency_ms", 
                             100 + hash(service) % 200, 
                             {"service": service}, "gauge")
    
    def record_metric(self, name: str, value: float, labels: Dict[str, str] = None, 
                     type: str = "gauge") -> str:
        """Record a metric."""
        if labels is None:
            labels = {}
        
        metric_id = str(uuid.uuid4())
        metric = Metric(name=name, value=value, labels=labels, type=type)
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO metrics (id, name, value, labels, timestamp, type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (metric_id, metric.name, metric.value, json.dumps(metric.labels),
                  metric.timestamp.isoformat(), metric.type))
            conn.commit()
        
        return metric_id
    
    def increment(self, name: str, labels: Dict[str, str] = None) -> str:
        """Increment a counter metric."""
        if labels is None:
            labels = {}
        return self.record_metric(name, 1.0, labels, "counter")
    
    def start_span(self, service: str, operation: str, trace_id: str = None, 
                   parent_span_id: str = None) -> str:
        """Start a new span."""
        if trace_id is None:
            trace_id = str(uuid.uuid4())
        
        span_id = str(uuid.uuid4())
        span = Span(trace_id=trace_id, span_id=span_id, parent_span_id=parent_span_id,
                   service=service, operation=operation)
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO spans (trace_id, span_id, parent_span_id, service, operation, 
                                  start_ts, end_ts, status, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (span.trace_id, span.span_id, span.parent_span_id, span.service, 
                  span.operation, span.start_ts.isoformat(), None, span.status, "{}"))
            conn.commit()
        
        return span_id
    
    def end_span(self, span_id: str, status: str = "ok", tags: Dict[str, str] = None) -> None:
        """End a span."""
        if tags is None:
            tags = {}
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                UPDATE spans SET end_ts = ?, status = ?, tags = ?
                WHERE span_id = ?
            """, (datetime.now().isoformat(), status, json.dumps(tags), span_id))
            conn.commit()
    
    def log(self, service: str, level: str, message: str, trace_id: str = None, **fields) -> str:
        """Record a log entry."""
        log_id = str(uuid.uuid4())
        log_entry = LogEntry(service=service, level=level, message=message, 
                            trace_id=trace_id, fields=fields)
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO logs (id, service, level, message, timestamp, trace_id, fields)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (log_id, log_entry.service, log_entry.level, log_entry.message,
                  log_entry.timestamp.isoformat(), log_entry.trace_id, 
                  json.dumps(log_entry.fields)))
            conn.commit()
        
        return log_id
    
    def get_metrics(self, name: str = None, labels: Dict[str, str] = None, 
                   since_minutes: int = 60) -> List[Dict]:
        """Query metrics."""
        if labels is None:
            labels = {}
        
        cutoff = datetime.now() - timedelta(minutes=since_minutes)
        
        query = "SELECT * FROM metrics WHERE timestamp > ?"
        params = [cutoff.isoformat()]
        
        if name:
            query += " AND name LIKE ?"
            params.append(f"%{name}%")
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_trace(self, trace_id: str) -> Dict[str, Any]:
        """Get full trace with all spans."""
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM spans WHERE trace_id = ?
                ORDER BY start_ts ASC
            """, (trace_id,))
            spans = [dict(row) for row in cursor.fetchall()]
        
        # Create waterfall view
        waterfall = []
        for span in spans:
            start = datetime.fromisoformat(span['start_ts'])
            end = datetime.fromisoformat(span['end_ts']) if span['end_ts'] else datetime.now()
            duration_ms = (end - start).total_seconds() * 1000
            waterfall.append({
                'service': span['service'],
                'operation': span['operation'],
                'duration_ms': duration_ms,
                'status': span['status']
            })
        
        return {'trace_id': trace_id, 'spans': spans, 'waterfall': waterfall}
    
    def get_logs(self, service: str = None, level: str = None, 
                 since_minutes: int = 60, limit: int = 100) -> List[Dict]:
        """Query logs."""
        cutoff = datetime.now() - timedelta(minutes=since_minutes)
        
        query = "SELECT * FROM logs WHERE timestamp > ?"
        params = [cutoff.isoformat()]
        
        if service:
            query += " AND service = ?"
            params.append(service)
        
        if level:
            query += " AND level = ?"
            params.append(level)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def service_dashboard(self, service: str) -> Dict[str, Any]:
        """Get service dashboard metrics."""
        metrics = self.get_metrics(f"{service}", since_minutes=60)
        logs = self.get_logs(service=service, level="error", since_minutes=60, limit=10)
        
        dashboard = {
            'service': service,
            'metrics': {
                'requests_per_minute': self._find_metric(metrics, f"{service}.requests_per_minute"),
                'error_rate': self._find_metric(metrics, f"{service}.error_rate"),
                'p50_latency': self._find_metric(metrics, f"{service}.p50_latency_ms"),
                'p95_latency': self._find_metric(metrics, f"{service}.p95_latency_ms"),
                'p99_latency': self._find_metric(metrics, f"{service}.p99_latency_ms"),
            },
            'recent_errors': logs
        }
        
        return dashboard
    
    def _find_metric(self, metrics: List[Dict], name: str) -> Optional[float]:
        """Find metric value by name."""
        for m in metrics:
            if m['name'] == name:
                return m['value']
        return None
    
    def alert_rules(self) -> List[Dict[str, Any]]:
        """Check metrics against thresholds and return violations."""
        violations = []
        metrics = self.get_metrics(since_minutes=5)
        
        for metric in metrics:
            # Error rate > 5%
            if 'error_rate' in metric['name'] and metric['value'] > 0.05:
                violations.append({
                    'metric': metric['name'],
                    'value': metric['value'],
                    'threshold': 0.05,
                    'severity': 'high'
                })
            
            # P99 latency > 500ms
            if 'p99_latency' in metric['name'] and metric['value'] > 500:
                violations.append({
                    'metric': metric['name'],
                    'value': metric['value'],
                    'threshold': 500,
                    'severity': 'medium'
                })
        
        return violations


def main():
    parser = argparse.ArgumentParser(description="Observability Platform")
    subparsers = parser.add_subparsers(dest="command")
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser("dashboard")
    dashboard_parser.add_argument("service", help="Service name")
    
    # Trace command
    trace_parser = subparsers.add_parser("trace")
    trace_parser.add_argument("trace_id", help="Trace ID")
    
    # Logs command
    logs_parser = subparsers.add_parser("logs")
    logs_parser.add_argument("--service", help="Service name")
    logs_parser.add_argument("--level", help="Log level")
    
    args = parser.parse_args()
    platform = ObservabilityPlatform()
    
    if args.command == "dashboard":
        dashboard = platform.service_dashboard(args.service)
        print(json.dumps(dashboard, indent=2))
    elif args.command == "trace":
        trace = platform.get_trace(args.trace_id)
        print(json.dumps(trace, indent=2, default=str))
    elif args.command == "logs":
        logs = platform.get_logs(service=args.service, level=args.level)
        for log in logs:
            print(f"[{log['timestamp']}] {log['service']} {log['level']}: {log['message']}")


if __name__ == "__main__":
    main()
