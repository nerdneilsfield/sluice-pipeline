from sluice.registry import register_sink_lazy

register_sink_lazy("telegram", "sluice.sinks.telegram:TelegramSink")
register_sink_lazy("feishu", "sluice.sinks.feishu:FeishuSink")
register_sink_lazy("email", "sluice.sinks.email:EmailSink")
