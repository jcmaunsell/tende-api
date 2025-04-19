import json
import logging
import os
from datetime import datetime

import requests
from ddtrace import tracer


class DatadogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.api_key = os.getenv('DD_API_KEY')
        self.service = os.getenv('DD_SERVICE', 'tende-api')
        self.env = os.getenv('DD_ENV', 'development')
        self.site = os.getenv('DD_SITE', 'datadoghq.com')
        self.url = f"https://http-intake.logs.{self.site}/api/v2/logs"
        
    def format_exception(self, exc_info):
        """Format exception info into a string."""
        if exc_info:
            return logging.Formatter().formatException(exc_info)
        return ""

    def emit(self, record):
        try:
            # Get current trace context if available
            span = tracer.current_span()
            trace_id = span.trace_id if span else None
            span_id = span.span_id if span else None
            
            # Create base log entry
            log_entry = {
                "timestamp": int(datetime.now().timestamp()),
                "status": record.levelname.lower(),
                "message": record.getMessage(),
                "service": self.service,
                "hostname": os.getenv('HOSTNAME', 'localhost'),
                "ddsource": "python",
                "ddtags": f"env:{self.env},service:{self.service}",
                "logger": {
                    "name": record.name,
                    "thread_name": record.threadName,
                    "method_name": record.funcName,
                    "line_number": record.lineno
                }
            }
            
            # Add trace context if available
            if trace_id and span_id:
                log_entry["dd"] = {
                    "trace_id": str(trace_id),
                    "span_id": str(span_id)
                }
            
            # Add exception info if present
            if record.exc_info:
                exc_type, exc_value, _ = record.exc_info
                log_entry["error"] = {
                    "kind": exc_type.__name__ if exc_type else None,
                    "message": str(exc_value) if exc_value else None,
                    "stack": self.format_exception(record.exc_info)
                }
            
            # Add extra fields if present
            if hasattr(record, 'extra'):
                log_entry["attributes"] = record.extra
            
            # Always print to stdout for Docker logs
            print(json.dumps(log_entry))
            
            # Only send to Datadog if API key is configured
            if self.api_key:
                headers = {
                    'Content-Type': 'application/json',
                    'DD-API-KEY': self.api_key
                }
                
                response = requests.post(
                    self.url,
                    headers=headers,
                    json=[log_entry],
                    timeout=2
                )
                response.raise_for_status()
                
        except Exception as e:
            # Print error to stdout but don't raise to avoid logging loops
            print(f"Failed to send log to Datadog: {str(e)}")

class DatadogFormatter(logging.Formatter):
    def format(self, record):
        if isinstance(record.msg, dict):
            return json.dumps(record.msg)
        return super().format(record)

# Configure the logger
dd_logger = logging.getLogger('tende')
dd_logger.setLevel(logging.DEBUG)

# Remove any existing handlers
for handler in dd_logger.handlers[:]:
    dd_logger.removeHandler(handler)

# Create and add handler
handler = DatadogHandler()
handler.setFormatter(DatadogFormatter())
dd_logger.addHandler(handler) 