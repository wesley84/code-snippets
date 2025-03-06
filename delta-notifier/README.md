# Delta Table Notifier

A Databricks utility that monitors Delta tables and sends notifications for table operations like creation, alteration, and partitioning events.

## Overview

This project provides automated monitoring and notifications for:
- Table creation events
- Schema alterations
- Partition management
- Data merge operations
- Updates and deletions
- Real-time change tracking

## Project Structure 

delta-notifier/
├── src/
│ └── delta_notifier/
│ ├── init.py
│ ├── table_monitor.py # Core monitoring functionality
│ └── handlers/
│ ├── init.py
│ ├── base.py # Base notification handler
│ ├── slack.py # Slack notifications
│ └── email.py # Email notifications
├── databricks.yml # Databricks job configuration
├── pyproject.toml # Python package configuration
├── README.md # Project documentation
└── .gitignore # Git ignore rules


## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.10+
- Poetry for dependency management
- Slack webhook URL (for Slack notifications)
- SMTP server access (for email notifications)

## Configuration

### Environment Setup

1. Configure Databricks authentication:

bash
~/.databrickscfg
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = your-personal-access-token

2. Install dependencies:

bash
pip install databricks-connect poetry
poetry install


### Notification Configuration

Configure notifications in `databricks.yml`:

yaml
notification_config:
slack:
webhook_url: "{{secrets.slack_webhook_url}}"
email:
smtp:
host: "smtp.example.com"
port: 587
use_tls: true
sender: "notifications@example.com"
username: "{{secrets.smtp_username}}"
password: "{{secrets.smtp_password}}"
recipients:
"team@example.com"


### Monitored Operations

Configure which operations to monitor:

operations_to_monitor:
"CREATE"
"ALTER"
"ADD PARTITION"
"DROP PARTITION"
"MERGE"
"UPDATE"
"DELETE"


## Usage

### 1. Build the Package

bash
cd delta-notifier
poetry build


### 2. Deploy to Databricks

bash
databricks bundle deploy --var="table_path=adb_cb6l9m_workspace.main.population_npl_2018-10-01" -t dev -p adb-wes-test


### 3. Run the Monitor

bash
databricks bundle run -t dev -p adb-wes-test


## Features

### Flexible Monitoring
- Monitor specific operations
- Configurable check intervals
- Multiple tables support
- Detailed change tracking

### Rich Notifications
- Operation-specific details
- User attribution
- Timestamp tracking
- Version history

### Multiple Notification Channels
- Slack integration
- Email notifications
- Extensible handler system

### Error Handling
- Robust error recovery
- Detailed logging
- Connection retry logic

## Development

### Adding New Handlers

1. Create a new handler class:

python
from delta_notifier.handlers.base import NotificationHandler
class CustomHandler(NotificationHandler):
def handle_change(self, changes):
# Implement notification logic
pass

2. Register the handler:

python
monitor = DeltaTableMonitor(spark, "table_path")
monitor.add_notification_handler(CustomHandler())
Tests

### Running Tests

bash
poetry run pytest


## Monitoring

Monitor the notification job in Databricks:
1. Go to Jobs tab
2. Find "Delta Table Monitor"
3. Check run status and logs

## Troubleshooting

Common issues and solutions:

1. **Notification Failures**
   - Verify webhook URLs
   - Check SMTP settings
   - Confirm network access

2. **Permission Issues**
   - Verify table access
   - Check Unity Catalog permissions
   - Confirm monitoring privileges

3. **Missing Events**
   - Verify operation types
   - Check interval settings
   - Confirm table changes

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Security

- Sensitive configuration uses Databricks secrets
- No credentials in code
- Secure communication channels
- Access control via Unity Catalog

## License

This project is licensed under the MIT License - see the LICENSE file for details.