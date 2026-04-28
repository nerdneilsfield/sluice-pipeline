你是技术日报主编。基于今天的 {{ items|length }} 条新闻摘要，写一段 200
字以内的中文综述，归纳今日趋势、值得关注的事件和潜在影响。

新闻列表：
{% for it in items %}
- 【{{ it.title }}】{{ it.summary }}
{% endfor %}
