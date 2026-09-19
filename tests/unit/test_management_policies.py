"""RabbitManagementClient policy endpoints (used to apply ``policy_templates()`` output)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.profiles import PolicyTemplate, policy_templates
from rabbitkit.management import RabbitManagementClient


def _client() -> tuple[RabbitManagementClient, MagicMock]:
    client = RabbitManagementClient()
    req = MagicMock(return_value=None)
    client._request = req  # type: ignore[method-assign]
    return client, req


class TestPolicies:
    def test_list_policies_all_and_vhost(self) -> None:
        client, req = _client()
        req.return_value = [{"name": "p"}]
        assert client.list_policies() == [{"name": "p"}]
        req.assert_called_with("GET", "/policies")
        client.list_policies("my/vhost")
        req.assert_called_with("GET", "/policies/my%2Fvhost")

    def test_get_policy(self) -> None:
        client, req = _client()
        req.return_value = {"name": "p"}
        assert client.get_policy("p", "/") == {"name": "p"}
        req.assert_called_with("GET", "/policies/%2F/p")

    def test_put_policy_with_raw_body(self) -> None:
        client, req = _client()
        body = {"pattern": "^q$", "definition": {"overflow": "reject-publish"}, "apply-to": "queues", "priority": 1}
        client.put_policy("p", body, vhost="/")
        method, path, raw = req.call_args[0]
        assert method == "PUT" and path == "/policies/%2F/p"
        assert json.loads(raw.decode()) == body

    def test_put_policy_with_template(self) -> None:
        client, req = _client()
        tpl = PolicyTemplate(name="rabbitkit-q-source", pattern="^q$", definition={"overflow": "reject-publish"})
        client.put_policy(tpl.name, tpl)
        method, path, raw = req.call_args[0]
        assert method == "PUT" and path == "/policies/%2F/rabbitkit-q-source"
        assert json.loads(raw.decode()) == tpl.as_api_body()

    def test_put_all_templates_from_policy_templates(self) -> None:
        client, req = _client()
        for tpl in policy_templates(["orders"], retry=RetryConfig(), profile="critical"):
            client.put_policy(tpl.name, tpl, vhost=tpl.vhost)
        assert req.call_count == 3
        names = {c[0][1] for c in req.call_args_list}
        assert names == {
            "/policies/%2F/rabbitkit-orders-source",
            "/policies/%2F/rabbitkit-orders-retry",
            "/policies/%2F/rabbitkit-orders-dlq",
        }

    def test_delete_policy(self) -> None:
        client, req = _client()
        client.delete_policy("p", "prod")
        req.assert_called_with("DELETE", "/policies/prod/p")


class TestCloseConnection:
    def test_close_connection(self) -> None:
        client, req = _client()
        client.close_connection("rabbit@host-1.2.3.4:5672 -> 5.6.7.8:9")
        method, path = req.call_args[0][:2]
        assert method == "DELETE"
        assert path == "/connections/rabbit%40host-1.2.3.4%3A5672%20-%3E%205.6.7.8%3A9"

    def test_close_connection_encodes_the_name(self) -> None:
        client, req = _client()
        client.close_connection("a/b c")
        assert req.call_args[0][1] == "/connections/a%2Fb%20c"
