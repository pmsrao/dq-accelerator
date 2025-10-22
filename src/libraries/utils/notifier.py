class Notifier:
    def __init__(self, sinks=None):
        self.sinks = sinks or []

    def send(self, title: str, message: str, severity: str = "info", extra=None):
        # Placeholder: plug Slack/Email/Jira here
        print(f"[NOTIFY][{severity}] {title}: {message}")
